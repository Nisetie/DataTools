using DataTools.Common;

namespace DataTools.DML
{
    /// <summary>
    /// Обертка вокруг sql-константы с вспомогательной информацией о типе данных
    /// </summary>
    public class SqlInsertConstant : ISqlExpression
    {
        public ISqlExpression Value { get; set; }
        public DBType ValueDBType { get; set; }
        public int? TextLength { get; set; }
        public int? NumericScale { get; set; }
        public int? NumericPrecision { get; set; }

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

