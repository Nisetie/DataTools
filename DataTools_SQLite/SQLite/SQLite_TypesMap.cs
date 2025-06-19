using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;

namespace DataTools.SQLite
{
    public unsafe static class SQLite_TypesMap
    {
        public const string INT = "INT";
        public const string REAL = "REAL";
        public const string TEXT = "TEXT";
        public const string BLOB = "BLOB";
        private static List<(string dataType, Type netType, DbType sqldbtype, object defaultValue)> _mapping;
        private static HashSet<string> _numbers;

        static SQLite_TypesMap()
        {
            _mapping = new List<(string dataType, Type netType, DbType sqldbtype, object defaultValue)>
            {
                (INT, typeof(bool), DbType.Boolean, false),
                (INT, typeof(byte), DbType.Byte, default(byte)),
                (INT, typeof(sbyte), DbType.SByte, default(sbyte)),
                (INT, typeof(short), DbType.Int16, default(short)),
                (INT, typeof(ushort), DbType.UInt16, default(ushort)),
                (INT, typeof(int), DbType.Int32, default(int)),
                (INT, typeof(uint), DbType.UInt32, default(uint)),
                (INT, typeof(long), DbType.Int64, default(long)),
                (INT, typeof(ulong), DbType.UInt64, default(ulong)),
                (REAL, typeof(float), DbType.Single, default(float)),
                (REAL, typeof(double), DbType.Double, default(double)),
                (REAL, typeof(decimal), DbType.Decimal, default(decimal)),
                (REAL, typeof(decimal), DbType.Currency, default(decimal)),
                (TEXT, typeof(char), DbType.String, default(char)),
                (TEXT, typeof(string), DbType.String, default(string)),
                (TEXT, typeof(DateTime), DbType.Date, default(DateTime)),
                (TEXT, typeof(TimeSpan), DbType.Time, default(TimeSpan)),
                (TEXT, typeof(DateTime), DbType.DateTime, default(DateTime)),
                (TEXT, typeof(DateTimeOffset), DbType.DateTimeOffset, default(DateTimeOffset)),
                (TEXT, typeof(Guid), DbType.Guid, default(string)),
                (BLOB, typeof(byte[]), DbType.Binary, default(byte[]))
            };

            _numbers = new HashSet<string>()
            {
                INT,
                REAL
            };
        }

        public static string GetSqlType(Type type)
        {
            var realType = Nullable.GetUnderlyingType(type);
            if (realType == null) realType = type;
            foreach (var el in _mapping)
                if (el.netType == realType)
                    return el.dataType;
            return TEXT;
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
            if (IsNumber(GetSqlType(value.GetType())))
                return $"{value}".Replace(',', '.');

            switch (value)
            {
                case DateTime dt:
                    if (dt.Year < 1)
                        dt = new DateTime(1, 1, 1, 0, 0, 0);
                    return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
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


