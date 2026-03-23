namespace DataTools.DML
{
    public class SqlCustom : SqlExpression
    {
        public string Query { get; private set; }
        public SqlCustom() { SetCustomQuery(""); }
        public SqlCustom(string customQuery) : base() => SetCustomQuery(customQuery);

        public SqlCustom SetCustomQuery(string customQuery)
        {
            Query = customQuery;
            PayloadLength = Query.Length;
            return this;
        }

        public override string ToString()
        {
            return Query;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlCustom sqlCustom)
                return Query == sqlCustom.Query;
            return false;
        }
    }
}

