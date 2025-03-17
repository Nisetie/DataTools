namespace DataTools.DML
{
    public class SqlCustom : SqlExpression
    {
        public string Query;
        public SqlCustom(string customQuery) => Query = customQuery;

        public override string ToString()
        {
            return Query;
        }
    }
}

