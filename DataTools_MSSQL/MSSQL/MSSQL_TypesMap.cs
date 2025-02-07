using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.MSSQL
{
    public unsafe static class MSSQL_TypesMap
    {
        private static List<(string sqltype, Type netType, SqlDbType sqldbtype, object defaultValue)> _mapping;
        private static HashSet<string> _numbers;

        static MSSQL_TypesMap()
        {
            _mapping = new List<(string sqltype, Type netType, SqlDbType sqldbtype, object defaultValue)>
            {
                ("tinyint", typeof(byte), SqlDbType.TinyInt, default(byte)),
                ("smallint", typeof(short), SqlDbType.SmallInt, default(short)),
                ("int", typeof(int), SqlDbType.Int, default(int)),
                ("bigint", typeof(long), SqlDbType.BigInt, default(long)),

                ("real", typeof(float), SqlDbType.Real, default(float)),
                ("float", typeof(double), SqlDbType.Float, default(double)),
                ("smallmoney", typeof(decimal), SqlDbType.SmallMoney, default(decimal)),
                ("money", typeof(decimal), SqlDbType.Money, default(decimal)),
                ("numeric", typeof(decimal), SqlDbType.Decimal, default(decimal)),
                ("decimal", typeof(decimal), SqlDbType.Decimal, default(decimal)),

                ("bit", typeof(bool), SqlDbType.Bit, default(bool)),

                ("char", typeof(string), SqlDbType.Char, default(string)),
                ("nchar", typeof(string), SqlDbType.NChar, default(string)),
                ("varchar", typeof(string), SqlDbType.VarChar, string.Empty),
                ("nvarchar", typeof(string), SqlDbType.NVarChar, default(string)),
                ("text", typeof(string), SqlDbType.Text, default(string)),
                ("ntext", typeof(string), SqlDbType.NText, default(string)),

                ("smalldatetime", typeof(DateTime), SqlDbType.DateTime, DateTime.Now),
                ("date", typeof(DateTime), SqlDbType.Date, DateTime.Now),
                ("datetime", typeof(DateTime), SqlDbType.DateTime, DateTime.Now),
                ("datetime2", typeof(DateTime), SqlDbType.DateTime2, DateTime.Now),
                ("datetimeoffset", typeof(DateTimeOffset), SqlDbType.DateTimeOffset, default(DateTimeOffset)),
                ("time", typeof(TimeSpan), SqlDbType.Time, default(TimeSpan)),
                ("timestamp", typeof(TimeSpan), SqlDbType.Timestamp, default(TimeSpan)),

                ("binary", typeof(byte[]), SqlDbType.VarBinary, default(byte[])),
                ("varbinary", typeof(byte[]), SqlDbType.VarBinary, default(byte[])),
                ("image", typeof(byte[]), SqlDbType.Binary, default(byte[])),
                ("rowversion", typeof(Byte[]), SqlDbType.Timestamp, default(byte[])),

                ("sql_variant", typeof(object), SqlDbType.Variant, default(object)),
                ("uniqueidentifier", typeof(Guid), SqlDbType.UniqueIdentifier, Guid.Empty),
                ("xml", typeof(string), SqlDbType.Xml, string.Empty)
            };

            _numbers = new HashSet<string>()
            {
                "bigint",
                //"bit",
                "decimal",
                "float",
                "int",
                "money",
                "numeric",
                "real",
                "smallint",
                "smallmoney",
                "tinyint"
            };
        }

        public static string GetSqlType(Type type)
        {
            if (type == typeof(string))
                return "nvarchar";
            foreach (var el in _mapping)
                if (el.netType == type)
                    return el.sqltype;
            return "nvarchar";
        }

        public static Type GetNetType(string sqlType)
        {
            sqlType = sqlType.ToLower();
            foreach (var el in _mapping)
            {
                if (sqlType == el.sqltype)
                    return el.netType;
            }
            return null;
        }

        public static bool IsNumber(string sqlType)
        {
            return _numbers.Contains(sqlType);
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

            if (IsNumber(GetSqlType(value.GetType())))
                return $"{value}".Replace(',', '.');

            switch (value)
            {
                case DateTime dt:
                    if (dt.Year < 1900)
                        dt = new DateTime(1900, 01, 01, 00, 00, 00);
                    return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
                case bool b:
                    return b ? "1" : "0";
                case byte[] byteArray:
                    return ByteArrayToHexViaLookup32UnsafeDirect(byteArray);
                default:
                    return $"'{value}'";

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

