namespace DataTools.DML
{
    public class SqlConstant : SqlExpression
    {
        private object _value;
        public object Value
        {
            get => _value;
            set
            {
                PayloadLength -= _value?.ToString().Length ?? 0;
                _value = value;
                PayloadLength += _value?.ToString().Length ?? 0;
            }
        }
        public SqlConstant(object value) => Value = value;

        public override string ToString()
        {
            return Value?.ToString() ?? "NULL";
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlConstant sqlConstant)
                return Value.Equals(sqlConstant.Value);
            else return false;
        }
    }
}

