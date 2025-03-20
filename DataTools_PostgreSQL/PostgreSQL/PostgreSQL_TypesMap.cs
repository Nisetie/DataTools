using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.PostgreSQL
{
    public unsafe static class PostgreSQL_TypesMap
    {
        private static List<(string sqltype, Type netType, NpgsqlDbType sqldbtype, object defaultValue)> _mapping;
        private static HashSet<string> _numbers;
        private static Dictionary<Type, (string sqltype, Type netType, NpgsqlDbType sqldbtype, object defaultValue)> _reverseMapping;
        static PostgreSQL_TypesMap()
        {
            _mapping = new List<(string sqltype, Type netType, NpgsqlDbType NpgsqlDbType, object defaultValue)>
            {
                ("boolean", typeof(bool), NpgsqlDbType.Boolean, default(bool)),
                ("smallint", typeof(short),NpgsqlDbType.Smallint, default(short)),
                ("int", typeof(int), NpgsqlDbType.Integer, default(int)),
                ("bigint", typeof(long), NpgsqlDbType.Bigint, default(long)),
                ("real", typeof(float), NpgsqlDbType.Real, default(float)),
                ("float", typeof(double), NpgsqlDbType.Double, default(double)),
                ("numeric", typeof(decimal), NpgsqlDbType.Numeric, default(decimal)),
                ("money", typeof(decimal), NpgsqlDbType.Money, default(decimal)),
                ("text", typeof(string), NpgsqlDbType.Text, string.Empty),
                ("json", typeof(string), NpgsqlDbType.Json, string.Empty),
                ("jsonb", typeof(string), NpgsqlDbType.Jsonb, string.Empty),
                ("xml", typeof(string), NpgsqlDbType.Xml, string.Empty),
                ("uuid", typeof(Guid), NpgsqlDbType.Uuid, Guid.Empty),
                ("bytea", typeof(byte[]), NpgsqlDbType.Bytea, default(byte[])),
                ("timestamp", typeof(DateTime), NpgsqlDbType.Timestamp, DateTime.Now),
                ("timestamptz", typeof(DateTimeOffset), NpgsqlDbType.TimestampTz, DateTimeOffset.Now),
                ("date", typeof(DateTime), NpgsqlDbType.Date, DateTime.Now),
                ("time", typeof(TimeSpan), NpgsqlDbType.Timestamp, TimeSpan.Zero),
                ("timetz", typeof(DateTimeOffset), NpgsqlDbType.TimestampTz, DateTimeOffset.Now),
                ("interval", typeof(TimeSpan), NpgsqlDbType.Timestamp, TimeSpan.Zero),
                ("bit", typeof(bool), NpgsqlDbType.Bit, default(bool)),
                ("char", typeof(char), NpgsqlDbType.Char, default(char))
            };

            _reverseMapping  = _mapping.ToDictionary(t => t.netType);

            _numbers = new HashSet<string>()
            {
                "smallint",
                "int",
                "bigint",
                "real",
                "float",
                "numeric",
                "money"
            };
        }

        public static string GetSqlType(Type type)
        {
            if (_reverseMapping.TryGetValue(type, out var t))
                return t.sqltype;
            //if (type == typeof(string))
            //    return "text";
            //foreach (var el in _mapping)
            //    if (el.netType == type)
            //        return el.sqltype;
            return "text";
        }

        public static bool IsNumber(string sqlType)
        {
            return _numbers.Contains(sqlType);
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
            var sqlType = GetSqlType(value.GetType()));
            if (IsNumber(sqlType))
                return $"{value}::{sqlType}".Replace(',', '.');

            switch (value)
            {
                case DateTime dt:
                    if (dt.Year < 1970)
                        dt = new DateTime(1970, 01, 01, 00, 00, 00);
                    return $"'{dt:yyyy-MM-dd HH:mm:ss}'::{sqlType}";
                case DateTimeOffset dto:
                    if (dto.Year < 1970)
                        dto = new DateTimeOffset(1970, 01, 01, 00, 00, 00, TimeSpan.Zero);
                    return $"'{dto:yyyy-MM-dd HH:mm:ss}'::{sqlType}";
                case byte[] byteArray:
                    return ByteArrayToHexViaLookup32UnsafeDirect(byteArray);
                default:
                    return $"'{value}'::{sqlType}";
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

