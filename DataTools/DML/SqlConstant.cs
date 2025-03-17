namespace DataTools.DML
{
    public class SqlConstant : SqlExpression
    {
        public object Value { get; set; }
        public SqlConstant(object value) => Value = value;

        public override string ToString()
        {
            return Value.ToString();
        }
    }
}

