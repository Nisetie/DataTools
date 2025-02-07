namespace DataTools.DML
{
    public class SqlOrderByClause : SqlExpression
    {
        public enum E_ORDER { ASC, DESC }

        public SqlExpression OrderValue;
        public E_ORDER Order;

        public SqlOrderByClause(SqlExpression expression, E_ORDER order = E_ORDER.ASC)
        {
            OrderValue = expression; Order = order;
        }
    }
}

