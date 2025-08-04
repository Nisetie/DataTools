using DataTools.Common;
using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.MSSQL
{
    public unsafe static class MSSQL_TypesMap
    {
        static MSSQL_TypesMap()
        {
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Boolean, "bit");

            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Binary, "varbinary", "binary", "image", "rowversion");

            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Guid, "uniqueidentifier");

            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Byte, "tinyint");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Int16, "smallint");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Int32, "int");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Int64, "bigint");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Single, "real");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Double, "float");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Money, "money", "smallmoney");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Decimal, "decimal", "numeric");

            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Timestamp, "datetime", "datetime2", "smalldatetime");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.TimestampTz, "datetimeoffset");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Date, "date");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Interval, "time");

            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.String, "nvarchar", "text");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.AnsiString, "varchar", "ntext");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.StringFixedLength, "nchar");
            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.AnsiStringFixedLength, "char");

            TypesMap.AddTypeLink(E_DBMS.MSSQL, DBType.Xml, "xml");

        }

        public static string GetSqlTypeFromType(Type type)
        {
            return GetSqlTypeFromDBType(DBType.GetDBTypeByType(type));
        }

        public static string GetSqlTypeFromDBType(DBType dbtype)
        {
            string sqltype;

            sqltype = TypesMap.GetSqlTypeFromDBType(E_DBMS.MSSQL, dbtype);
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

