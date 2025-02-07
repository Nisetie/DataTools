namespace DataTools.DML
{
    public class SqlName : SqlExpression
    {
        public string Name;
        public SqlName(string name) => Name = name;
    }
}

