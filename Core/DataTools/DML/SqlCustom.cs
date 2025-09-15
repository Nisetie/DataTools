namespace DataTools.DML
{
    public class SqlCustom : ISqlExpression
    {
        public string Query;
        public SqlCustom() { Query = ""; }
        public SqlCustom(string customQuery) : base() => Query = customQuery;

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

