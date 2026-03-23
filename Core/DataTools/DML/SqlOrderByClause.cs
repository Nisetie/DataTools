namespace DataTools.DML
{
    public class SqlOrderByClause : SqlExpression
    {
        public enum E_ORDER { ASC, DESC }

        public ISqlExpression OrderValue { get; private set; }
        public E_ORDER Order { get; private set; }

        public SqlOrderByClause(ISqlExpression expression, E_ORDER order = E_ORDER.ASC)
        {
            OrderBy(expression, order);
        }

        public SqlOrderByClause OrderBy (ISqlExpression expression, E_ORDER order = E_ORDER.ASC)
        {
            OrderValue = expression; 
            Order = order;
            PayloadLength = (OrderValue?.PayloadLength ?? 0) + order.ToString().Length;
            return this;
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

