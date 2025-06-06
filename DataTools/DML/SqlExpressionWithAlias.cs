namespace DataTools.DML
{
    public class SqlExpressionWithAlias : SqlExpression
    {
        public SqlExpression SqlExpression;
        public string Alias { get; set; }
        public SqlExpressionWithAlias(SqlExpression expression, string alias)
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

