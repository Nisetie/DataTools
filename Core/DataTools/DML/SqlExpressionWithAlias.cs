namespace DataTools.DML
{
    public class SqlExpressionWithAlias : SqlExpression
    {
        public ISqlExpression SqlExpression { get; private set; }
        public string Alias { get; private set; }
        public SqlExpressionWithAlias(ISqlExpression expression, string alias)
        {
            SetExpression(expression, alias);
        }

        public SqlExpressionWithAlias SetExpression(ISqlExpression expression, string alias)
        {
            SqlExpression = expression;
            Alias = alias;
            PayloadLength = (SqlExpression?.PayloadLength ?? 0) + (alias?.Length ?? 0);
            return this;
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

