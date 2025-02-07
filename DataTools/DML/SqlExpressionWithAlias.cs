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
    }
}

