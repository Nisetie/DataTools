using DataTools.Common;
using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.PostgreSQL
{
    public unsafe static class PostgreSQL_TypesMap
    {
        static PostgreSQL_TypesMap()
        {
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Boolean, "bool", "boolean");

            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Binary, "bytea");

            TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.Byte, "smallint");
            TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.SByte, "smallint");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Int16, "smallint", "int2", "smallserial");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Int32, "int", "integer", "int4", "serial");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Int64, "bigint", "int8", "bigserial");
            TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.UInt16, "int");
            TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.UInt32, "bigint");
            TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.UInt64, "bigint");

            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Json, "json");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Xml, "xml");

            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Single, "real", "float4");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Double, "float", "double precision", "float8");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Money, "money");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Decimal, "numeric", "decimal");

            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.String, "text", "character varying", "varchar", "bpchar");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.StringFixedLength, "char", "character");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Char, "char");

            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Date, "date");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Time, "time without time zone");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Timestamp, "timestamp without time zone");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.TimestampTz, "timestamptz");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Interval, "interval");
            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.TimeTz, "timetz");

            TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Guid, "uuid");
        }

        public static string GetSqlType(Type type)
        {
            return GetSqlType(DBType.GetDBTypeByType(type));
        }

        public static string GetSqlType(DBType dbtype)
        {
            string sqltype;
            sqltype = TypesMap.GetSqlTypeFromDBType(E_DBMS.PostgreSQL, dbtype);
            return sqltype;
        }

        public static Type GetNetType(string sqlType)
        {
            return GetDBType(sqlType).Type;
        }

        public static DBType GetDBType(string sqlType)
        {
            sqlType = sqlType.Split('(')[0].ToLower();
            DBType type = TypesMap.GetDBTypeFromSqlType(E_DBMS.PostgreSQL, sqlType);
            return type;
        }

        /// <summary>
        /// Добавить обрамление, если формат SQL того требует в запросе (для строковых литералов)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string ToStringSQL(object value)
        {
            if (value == null)
                return "NULL";

            var dbType = DBType.GetDBTypeByType(value.GetType());
            var sqlType = GetSqlType(dbType);
            if (dbType.IsNumber)
                return $"({value.ToString().Replace(',', '.')})::{sqlType}";            

            switch (value)
            {
                case DateTime dt:
                    if (dt.Year < 1970)
                        dt = new DateTime(1970, 01, 01, 00, 00, 00);
                    return $"('{dt:yyyy-MM-dd HH:mm:ss.fff}')::{sqlType}";
                case DateTimeOffset dto:
                    if (dto.Year < 1970)
                        dto = new DateTimeOffset(1970, 01, 01, 00, 00, 00, TimeSpan.Zero);
                    dto = dto.ToUniversalTime();
                    return $"('{dto:o}')::{sqlType}";
                case byte[] byteArray:
                    return $"({ByteArrayToHexViaLookup32UnsafeDirect(byteArray)})::bytea";
                default:
                    return $"('{value.ToString().Replace("'", "''")}')::{sqlType}";
            }
        }

        // https://stackoverflow.com/questions/311165/how-do-you-convert-a-byte-array-to-a-hexadecimal-string-and-vice-versa
        private static uint[] _Lookup32 = Enumerable.Range(0, 256).Select(i =>
        {
            string s = i.ToString("X2");
            if (BitConverter.IsLittleEndian)
                return s[0] + ((uint)s[1] << 16);
            else
                return s[1] + ((uint)s[0] << 16);
        }).ToArray();
        private static uint* _lookup32UnsafeP = (uint*)GCHandle.Alloc(_Lookup32, GCHandleType.Pinned).AddrOfPinnedObject();
        public static string ByteArrayToHexViaLookup32UnsafeDirect(byte[] bytes)
        {
            var lookupP = _lookup32UnsafeP;
            var result = new string((char)0, bytes.Length * 2 + 4);
            fixed (byte* bytesP = bytes)
            fixed (char* resultP = result)
            {
                resultP[0] = '\'';
                resultP[1] = '\\';
                resultP[2] = 'x';
                resultP[bytes.Length * 2 + 3] = '\'';
                uint* resultP2 = (uint*)(resultP + 3);
                for (int i = 0; i < bytes.Length; i++)
                {
                    resultP2[i] = lookupP[bytesP[i]];
                }
            }
            return result;
        }
    }
}

