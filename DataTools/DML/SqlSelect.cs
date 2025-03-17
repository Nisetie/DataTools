using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlSelect : SqlExpression
    {
        private static readonly SqlConstant _zeroOffset = new SqlConstant(0);
        private static readonly SqlConstant _maxLimit = new SqlConstant(long.MaxValue);
        private static readonly SqlOrderByClause[] _emptyOrderBy = new SqlOrderByClause[0];
        private static readonly SqlWhereClause _dummyWhere = new SqlWhereClause();
        private static readonly SqlExpression[] _emptySelect = new SqlExpression[0];

        protected SqlExpression _from = null;
        protected IEnumerable<SqlExpression> _selects = _emptySelect;
        protected SqlWhereClause _where = _dummyWhere;
        protected IEnumerable<SqlOrderByClause> _orders = _emptyOrderBy;
        protected SqlExpression _offset = _zeroOffset;
        protected SqlExpression _limit = _maxLimit;

        public IEnumerable<SqlExpression> Selects => _selects;
        public SqlExpression FromSource => _from;
        public SqlWhereClause Wheres => _where;
        public IEnumerable<SqlOrderByClause> Orders => _orders;
        public SqlExpression OffsetRows => _offset;
        public SqlExpression LimitRows => _limit;

        public SqlSelect Select(params SqlExpression[] selects)
        {
            _selects = selects;
            return this;
        }
        public SqlSelect From(SqlExpression objectName)
        {
            _from = objectName;
            return this;
        }
        public SqlSelect Where(SqlWhereClause where)
        {
            _where = where;
            return this;
        }
        public SqlSelect OrderBy(params SqlOrderByClause[] order)
        {
            _orders = order;
            return this;
        }
        public SqlSelect Offset(SqlExpression offset)
        {
            _offset = offset;
            return this;
        }
        public SqlSelect Limit(SqlExpression limit)
        {
            _limit = limit;
            return this;
        }
    }
}

