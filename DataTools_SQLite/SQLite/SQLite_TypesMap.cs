using DataTools.Common;
using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.SQLite
{
    public unsafe static class SQLite_TypesMap
    {
        static SQLite_TypesMap()
        {
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Binary, "blob");

            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Boolean, "int");

            

            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Byte, "tinyint");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.SByte, "smallint");
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Int16, "smallint");
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Int32, "integer", "int");
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Int64, "bigint");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.UInt16, "int");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.UInt32, "bigint");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.UInt64, "unsigned big int");
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Single, "float");
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Double, "double", "double precision");
            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Decimal, "numeric", "decimal");

            TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.String, "text", "varchar", "nvarchar", "varying character", "clob", "nchar");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.StringFixedLength, "text");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.AnsiString, "text");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.AnsiStringFixedLength, "text");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Char, "nchar");

            // хранить всё в текстовом виде в формате UTC, т.к. стандарт
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Time, "nvarchar(64)");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Date, "nvarchar(64)");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Timestamp, "nvarchar(64)");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.TimestampTz, "nvarchar(64)");

            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Guid, "nvarchar(64)");

            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Json, "text");
            TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Xml, "text");
        }

        public static string GetSqlType(Type type)
        {
            return GetSqlType(DBType.GetDBTypeByType(type));
        }

        public static string GetSqlType(DBType dbtype)
        {
            string sqltype;
            sqltype = TypesMap.GetSqlTypeFromDBType(E_DBMS.SQLite, dbtype);
            return sqltype;
        }
        public static Type GetNetType(string sqlType)
        {
            sqlType = sqlType.Split('(')[0].ToLower();
            DBType type = TypesMap.GetDBTypeFromSqlType(E_DBMS.SQLite, sqlType);
            return type.Type;
        }

        public static DBType GetDBType(string sqlType)
        {
            sqlType = sqlType.Split('(')[0].ToLower();
            DBType type = TypesMap.GetDBTypeFromSqlType(E_DBMS.SQLite, sqlType);
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
            else
            {
                if (DBType.GetDBTypeByType(value.GetType()).IsNumber)
                    return $"{value}".Replace(',', '.');
                else
                    switch (value)
                    {
                        case DateTime dt:
                            return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
                        case DateTimeOffset dto:
                            return $"'{dto:o}'";
                        case bool b:
                            return b ? "1" : "0";
                        case byte[] byteArray:
                            return ByteArrayToHexViaLookup32UnsafeDirect(byteArray);
                        default:
                            return $"'{value.ToString().Replace("'", "''")}'";
                    }
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
            var result = new string((char)0, bytes.Length * 2 + 3);
            fixed (byte* bytesP = bytes)
            fixed (char* resultP = result)
            {
                resultP[0] = 'x';
                resultP[1] = '\'';
                resultP[bytes.Length * 2 + 2] = '\'';
                uint* resultP2 = (uint*)(resultP + 2);
                for (int i = 0; i < bytes.Length; i++)
                {
                    resultP2[i] = lookupP[bytesP[i]];
                }
            }
            return result;
        }
    }
}



