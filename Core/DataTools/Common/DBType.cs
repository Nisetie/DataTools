using DataTools.Interfaces;
using System;
using System.Collections.Generic;

namespace DataTools.Common
{
    /// <summary>
    /// Обобщенный абстратный тип данных-посредник между схожими типами во всех СУБД.
    /// </summary>
    public class DBType : Enumeration
    {
        public Type Type { get; private set; }
        public bool HasLength { get; private set; }
        public bool IsNumber { get; private set; }
        public bool IsText { get; private set; }

        public bool HasPrecision { get; private set; }

        private static int _iterator = 0;
        private static int _GetId() => _iterator++;

        private static Dictionary<string, DBType> _typeNames = new Dictionary<string, DBType>();
        private static Dictionary<int, DBType> _typeIds = new Dictionary<int, DBType>();
        private static Dictionary<Type, DBType> _typeTypes = new Dictionary<Type, DBType>();

        // Boolean Type
        public static DBType Boolean = new DBType(nameof(Boolean), typeof(bool), hasTypePriority: true);

        // Numeric Types
        public static DBType Byte = new DBType(nameof(Byte), typeof(byte), hasTypePriority: true, isNumber: true);
        public static DBType SByte = new DBType(nameof(SByte), typeof(sbyte), hasTypePriority: true, isNumber: true);
        public static DBType Int16 = new DBType(nameof(Int16), typeof(short), hasTypePriority: true, isNumber: true);
        public static DBType Int32 = new DBType(nameof(Int32), typeof(int), hasTypePriority: true, isNumber: true);
        public static DBType Int64 = new DBType(nameof(Int64), typeof(long), hasTypePriority: true, isNumber: true);
        public static DBType UInt16 = new DBType(nameof(UInt16), typeof(ushort), hasTypePriority: true, isNumber: true);
        public static DBType UInt32 = new DBType(nameof(UInt32), typeof(uint), hasTypePriority: true, isNumber: true);
        public static DBType UInt64 = new DBType(nameof(UInt64), typeof(ulong), hasTypePriority: true, isNumber: true);

        public static DBType Single = new DBType(nameof(Single), typeof(float), hasTypePriority: true, isNumber: true);
        public static DBType Double = new DBType(nameof(Double), typeof(double), hasTypePriority: true, isNumber: true);

        public static DBType Decimal = new DBType(nameof(Decimal), typeof(decimal), hasTypePriority: true, hasPrecision: true, isNumber: true);
        public static DBType Money = new DBType(nameof(Money), typeof(decimal), hasPrecision: true, isNumber: true);

        // Character Types
        public static DBType String = new DBType(nameof(String), typeof(string), hasLength: true, hasTypePriority: true, isText: true);
        public static DBType AnsiString = new DBType(nameof(AnsiString), typeof(string), hasLength: true, isText: true);
        public static DBType AnsiStringFixedLength = new DBType(nameof(AnsiStringFixedLength), typeof(string), hasLength: true, isText: true);
        public static DBType StringFixedLength = new DBType(nameof(StringFixedLength), typeof(string), hasLength: true, isText: true);
        public static DBType Char = new DBType(nameof(Char), typeof(char), hasTypePriority: true, isText: true);
        //// XML
        public static DBType Xml = new DBType(nameof(Xml), typeof(string));
        //// JSON Types
        public static DBType Json = new DBType(nameof(Json), typeof(string));

        // GUID
        public static DBType Guid = new DBType(nameof(Guid), typeof(Guid), hasTypePriority: true);

        // Binary Data Types
        public static DBType Binary = new DBType(nameof(Binary), typeof(byte[]), hasTypePriority: true, hasLength: true);

        // Date/Time Types
        public static DBType Timestamp = new DBType(nameof(Timestamp), typeof(DateTime), hasTypePriority: true);
        public static DBType Date = new DBType(nameof(Date), typeof(DateTime));
        public static DBType Time = new DBType(nameof(Time), typeof(TimeSpan), hasTypePriority: true);
        public static DBType TimestampTz = new DBType(nameof(TimestampTz), typeof(DateTimeOffset), hasTypePriority: true);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name">Общее наименование типа</param>
        /// <param name="type">Тип в C#</param>
        /// <param name="hasLength">Тип (текстовый) может иметь фиксированную длину</param>
        /// <param name="hasPrecision">Тип (числовой) может иметь длину и точность</param>
        /// <param name="hasTypePriority">Приоритетное сопоставление для <paramref name="type"/></param>
        public DBType(string name, Type type, bool isNumber = false, bool isText = false, bool hasLength = false, bool hasPrecision = false, bool hasTypePriority = false)
            : base(_GetId(), name)
        {
            _typeNames[Name] = this;
            _typeIds[Id] = this;
            Type = type;
            if (hasTypePriority)
                _typeTypes[type] = this;
            HasLength = hasLength;
            HasPrecision = hasPrecision;
            IsNumber = isNumber;
            IsText = isText;
        }

        public override string ToString()
        {
            return Name.ToString();
        }

        public static DBType GetDBTypeById(int typeId)
        {
            if (_typeIds.TryGetValue(typeId, out var dbtype))
                return dbtype;
            else return null;
        }
        public static DBType GetDBTypeByName(string typename)
        {
            if (_typeNames.TryGetValue(typename, out var dbtype))
                return dbtype;
            else return null;
        }

        public static DBType GetDBTypeByType(Type type)
        {
            if (_typeTypes.TryGetValue(type, out var dbtype))
                return dbtype;
            else return null;
        }
    }
}

