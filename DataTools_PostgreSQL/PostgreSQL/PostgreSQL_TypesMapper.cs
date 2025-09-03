using DataTools.Common;
using DataTools.Interfaces;
using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.PostgreSQL
{
    public unsafe class PostgreSQL_TypesMapper : TypesMapper
    {
        private static PostgreSQL_TypesMapper _instance;
        private PostgreSQL_TypesMapper() : base() { }
        static PostgreSQL_TypesMapper()
        {
            _instance = new PostgreSQL_TypesMapper();
        }
        protected override void AddLinkBoolean() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Boolean, "bool", "boolean");
        protected override void AddLinkBinary() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Binary, "bytea");
        protected override void AddLinkGuid() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Guid, "uuid");
        protected override void AddLinkByte() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.Byte, "smallint");
        protected override void AddLinkSByte() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.SByte, "smallint");
        protected override void AddLinkInt16() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Int16, "smallint", "int2", "smallserial");
        protected override void AddLinkInt32() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Int32, "int", "integer", "int4", "serial");
        protected override void AddLinkInt64() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Int64, "bigint", "int8", "bigserial");
        protected override void AddLinkUInt16() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.UInt16, "int");
        protected override void AddLinkUInt32() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.UInt32, "bigint");
        protected override void AddLinkUInt64() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.UInt64, "bigint");
        protected override void AddLinkSingle() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Single, "real", "float4");
        protected override void AddLinkDouble() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Double, "float", "double precision", "float8");
        protected override void AddLinkMoney() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Money, "money");
        protected override void AddLinkDecimal() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Decimal, "numeric", "decimal");
        protected override void AddLinkTimestamp() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Timestamp, "timestamp without time zone", "timestamp");
        protected override void AddLinkTimestampTz() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.TimestampTz, "timestamptz");
        protected override void AddLinkDate() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Date, "date");
        protected override void AddLinkTime() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Time, "time without time zone", "time");
        protected override void AddLinkString() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.String, "text", "character varying", "varchar", "bpchar");
        protected override void AddLinkAnsiString() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.AnsiString, "text");
        protected override void AddLinkStringFixedLength() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.StringFixedLength, "char", "character");
        protected override void AddLinkAnsiStringFixedLength() => TypesMap.AddForwardLinkOnly(E_DBMS.PostgreSQL, DBType.AnsiStringFixedLength, "varchar");
        protected override void AddLinkChar() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Char, "char");
        protected override void AddLinkJson() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Json, "json");
        protected override void AddLinkXml() => TypesMap.AddTypeLink(E_DBMS.PostgreSQL, DBType.Xml, "xml");

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

