namespace DataTools.DML
{
    public class SqlExpressionWithAlias : ISqlExpression
    {
        public ISqlExpression SqlExpression;
        public string Alias { get; set; }
        public SqlExpressionWithAlias(ISqlExpression expression, string alias)
        {
            SqlExpression = expression;
            Alias = alias;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlExpressionWithAlias sqlExpression)
                return Alias == sqlExpression.Alias && SqlExpression.Equals(sqlExpression.SqlExpression);
            return false;
        }

        public override string ToString()
        {
            return $"({SqlExpression}) AS {Alias}";
        }
    }
}

