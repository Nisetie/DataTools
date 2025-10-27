using DataTools.Common;
using DataTools.Interfaces;
using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.MSSQL
{
    public unsafe class MSSQL_TypesMapper : TypesMapper
    {
        private static MSSQL_TypesMapper _instance;
        private MSSQL_TypesMapper() :base() { }

        protected override void AddLinkBoolean() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Boolean, "bit");
        protected override void AddLinkBinary() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Binary, "varbinary", "binary", "image", "rowversion");
        protected override void AddLinkGuid() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Guid, "uniqueidentifier");
        protected override void AddLinkByte() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Byte, "tinyint");
        protected override void AddLinkSByte() => TypesMap.AddForwardLinkOnly(E_DBMS.MSSQL, DBType.SByte, "smallint");
        protected override void AddLinkInt16() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Int16, "smallint");
        protected override void AddLinkInt32() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Int32, "int");
        protected override void AddLinkInt64() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Int64, "bigint");
        protected override void AddLinkUInt16() => TypesMap.AddForwardLinkOnly(E_DBMS.MSSQL, DBType.UInt16, "int");
        protected override void AddLinkUInt32() => TypesMap.AddForwardLinkOnly(E_DBMS.MSSQL, DBType.UInt32, "int");
        protected override void AddLinkUInt64() => TypesMap.AddForwardLinkOnly(E_DBMS.MSSQL, DBType.UInt64, "bigint");
        protected override void AddLinkSingle() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Single, "real");
        protected override void AddLinkDouble() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Double, "float");
        protected override void AddLinkMoney() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Money, "money", "smallmoney");
        protected override void AddLinkDecimal() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Decimal, "decimal", "numeric");
        protected override void AddLinkTimestamp() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Timestamp, "datetime", "datetime2", "smalldatetime");
        protected override void AddLinkTimestampTz() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.TimestampTz, "datetimeoffset");
        protected override void AddLinkDate() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Date, "date");
        protected override void AddLinkTime() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Time, "time");
        protected override void AddLinkString() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.String, "nvarchar", "text");
        protected override void AddLinkAnsiString() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.AnsiString, "varchar", "ntext");
        protected override void AddLinkStringFixedLength() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.StringFixedLength, "nchar");
        protected override void AddLinkAnsiStringFixedLength() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.AnsiStringFixedLength, "char");
        protected override void AddLinkChar() => TypesMap.AddForwardLinkOnly(E_DBMS.MSSQL, DBType.Char, "char");
        protected override void AddLinkJson() => TypesMap.AddForwardLinkOnly(E_DBMS.MSSQL, DBType.Json, "text");
        protected override void AddLinkXml() => TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Xml, "xml");
        static MSSQL_TypesMapper()
        {
            _instance = new MSSQL_TypesMapper();            
        }
        /// <summary>
        /// Получить название sql-типа в СУБД MSSQL для .NET типа данных.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string GetSqlTypeFromType(Type type)
        {
            return GetSqlType(DBType.GetDBTypeByType(type));
        }
        /// <summary>
        /// Получить название sql-типа в СУБД MSSQL для абстрактного <paramref name="dbtype"/> типа данных.
        /// </summary>
        /// <param name="dbtype"></param>
        /// <returns></returns>
        public static string GetSqlType(DBType dbtype)
        {
            string sqltype;

            sqltype = TypesMap.GetSqlType(E_DBMS.MSSQL, dbtype);
            return sqltype;
        }

        public static Type GetNetTypeFromSqlType(string sqlType)
        {
            return GetDBTypeFromSqlType(sqlType).Type;
        }

        public static DBType GetDBTypeFromSqlType(string sqlType)
        {
            sqlType = sqlType.Split('(')[0].ToLower();
            DBType type = TypesMap.GetDBTypeFromSqlType(E_DBMS.MSSQL, sqlType);
            return type;
        }

        /// <summary>
        /// Форматирование значения в вид, удобоваримый источником.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string ToStringSQL(object value)
        {
            if (value == null)
                return "NULL";

            if (DBType.GetDBTypeByType(value.GetType()).IsNumber)
                return $"{value}".Replace(',', '.');

            switch (value)
            {
                case DateTime dt:
                    if (dt.Year < 1900)
                        dt = new DateTime(1900, 01, 01, 00, 00, 00, DateTimeKind.Unspecified);
                    return $"'{dt:yyyy-MM-ddTHH:mm:ss.fff}'";
                case DateTimeOffset dto:
                    if (dto.Year < 1970)
                        dto = new DateTimeOffset(1970, 01, 01, 00, 00, 00, TimeSpan.Zero);
                    return $"'{dto:o}'";
                case bool b:
                    return b ? "1" : "0";
                case byte[] byteArray:
                    return ByteArrayToHexViaLookup32UnsafeDirect(byteArray);
                default:
                    return $"'{value.ToString().Replace("'", "''")}'";

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
        /// <summary>
        /// Преобразовать байтовый массив в строковый литерал
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static string ByteArrayToHexViaLookup32UnsafeDirect(byte[] bytes)
        {
            var lookupP = _lookup32UnsafeP;
            var result = new string((char)0, bytes.Length * 2 + 2);
            fixed (byte* bytesP = bytes)
            fixed (char* resultP = result)
            {
                resultP[0] = '0';
                resultP[1] = 'x';
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

