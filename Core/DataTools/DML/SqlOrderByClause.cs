namespace DataTools.DML
{
    public class SqlOrderByClause : ISqlExpression
    {
        public enum E_ORDER { ASC, DESC }

        public ISqlExpression OrderValue;
        public E_ORDER Order;

        public SqlOrderByClause(ISqlExpression expression, E_ORDER order = E_ORDER.ASC)
        {
            OrderValue = expression; Order = order;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlOrderByClause sqlOrderByClause)
                return OrderValue.Equals(sqlOrderByClause.OrderValue) && Order == sqlOrderByClause.Order;
            return false;
        }

        public override string ToString()
        {
            return $"{OrderValue} {Order}";
        }
    }
}

