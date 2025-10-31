using DataTools.Common;

namespace DataTools.DML
{
    /// <summary>
    /// Обертка вокруг sql-константы с вспомогательной информацией о типе данных
    /// </summary>
    public class SqlInsertConstant : SqlExpression
    {
        private ISqlExpression _value;
        private DBType _valueDBType;
        private int? _textlength;
        private int? _numericScale;
        private int? _numericPrecision;
        public ISqlExpression Value
        {
            get => _value;
            set
            {
                PayloadLength -= _value?.PayloadLength ?? 0;
                _value = value;
                PayloadLength += _value?.PayloadLength ?? 0;
            }
        }
        public DBType ValueDBType
        {
            get => _valueDBType;
            set
            {
                PayloadLength -= _valueDBType?.ToString().Length ?? 0;
                _valueDBType = value;
                PayloadLength += _valueDBType?.ToString().Length ?? 0;
            }
        }
        public int? TextLength
        {
            get => _textlength;
            set
            {
                PayloadLength -= _textlength?.ToString().Length ?? 0;
                _textlength = value;
                PayloadLength += _textlength?.ToString().Length ?? 0;
            }
        }
        public int? NumericScale
        {
            get => _numericScale;
            set
            {
                PayloadLength -= _numericScale?.ToString().Length ?? 0;
                _numericScale = value;
                PayloadLength += _numericScale?.ToString().Length ?? 0;
            }
        }
        public int? NumericPrecision
        {
            get => _numericPrecision;
            set
            {
                PayloadLength -= _numericPrecision?.ToString().Length ?? 0;
                _numericPrecision = value;
                PayloadLength += _numericPrecision?.ToString().Length ?? 0;
            }
        }

        public SqlInsertConstant(ISqlExpression value, DBType valueDBType)
        {
            Value = value;
            ValueDBType = valueDBType;
        }

        public override string ToString()
        {
            if (ValueDBType.IsNumber)
            {
                if (NumericPrecision != null)
                    return $"{Value} as {ValueDBType}({NumericScale},{NumericPrecision})";
                else
                    return $"{Value} as {ValueDBType}";
            }
            else if (ValueDBType.IsText)
            {
                if (TextLength != null)
                    return $"{Value} as{ValueDBType}({TextLength})";
                else
                    return $"{Value} as {ValueDBType}";
            }
            else
                return $"{Value} as {ValueDBType}";

        }
    }
}

