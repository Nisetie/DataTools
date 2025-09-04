using DataTools.Common;
using DataTools.Interfaces;
using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.SQLite
{
    public unsafe class SQLite_TypesMapper : TypesMapper
    {
        private static SQLite_TypesMapper _instance;
        private SQLite_TypesMapper() : base() { }
        static SQLite_TypesMapper()
        {
            _instance = new SQLite_TypesMapper();
        }

        protected override void AddLinkBoolean() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Boolean, "int");
        protected override void AddLinkBinary() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Binary, "blob");
        protected override void AddLinkGuid() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Guid, "nvarchar(64)");
        protected override void AddLinkByte() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Byte, "tinyint");
        protected override void AddLinkSByte() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.SByte, "smallint");
        protected override void AddLinkInt16() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Int16, "smallint");
        protected override void AddLinkInt32() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Int32, "integer", "int");
        protected override void AddLinkInt64() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Int64, "bigint");
        protected override void AddLinkUInt16() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.UInt16, "int");
        protected override void AddLinkUInt32() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.UInt32, "bigint");
        protected override void AddLinkUInt64() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.UInt64, "unsigned big int");
        protected override void AddLinkSingle() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Single, "float");
        protected override void AddLinkDouble() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Double, "double", "double precision");
        protected override void AddLinkMoney() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Money, "numeric");
        protected override void AddLinkDecimal() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.Decimal, "numeric", "decimal");
        protected override void AddLinkTimestamp() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Timestamp, "nvarchar(64)");
        protected override void AddLinkTimestampTz() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.TimestampTz, "nvarchar(64)");
        protected override void AddLinkDate() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Date, "nvarchar(64)");
        protected override void AddLinkTime() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Time, "nvarchar(64)");
        protected override void AddLinkString() => TypesMap.AddTypeLink(E_DBMS.SQLite, DBType.String, "text", "varchar", "nvarchar", "varying character", "clob", "nchar");
        protected override void AddLinkAnsiString() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.AnsiString, "text");
        protected override void AddLinkStringFixedLength() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.StringFixedLength, "text");
        protected override void AddLinkAnsiStringFixedLength() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.AnsiStringFixedLength, "text");
        protected override void AddLinkChar() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Char, "nchar");
        protected override void AddLinkJson() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Json, "text");
        protected override void AddLinkXml() => TypesMap.AddForwardLinkOnly(E_DBMS.SQLite, DBType.Xml, "text");

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



