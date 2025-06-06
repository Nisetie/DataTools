using System.Collections.Generic;
using System.Text;

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


        public override bool Equals(object obj)
        {
            if (obj is SqlSelect sqlSelect)
            {
                if (!_from.Equals(sqlSelect._from)
                    || !_where.Equals(sqlSelect._where)
                    || !_offset.Equals(sqlSelect._offset)
                    || !_limit.Equals(sqlSelect._limit))
                    return false;
                var leftSE = _selects.GetEnumerator();
                var rightSE = sqlSelect._selects.GetEnumerator();
                while (leftSE.MoveNext())
                    if (!rightSE.MoveNext() || !leftSE.Current.Equals(rightSE.Current)) return false;
                var leftOE = _orders.GetEnumerator();
                var rightOE = sqlSelect._orders.GetEnumerator();
                while (leftOE.MoveNext())
                    if (!rightOE.MoveNext() || !leftOE.Current.Equals(rightOE.Current)) return false;
                return true;
            }
            return false;
        }

        public override string ToString()
        {
//FROM and/ or JOIN clause.
//WHERE clause.
//GROUP BY clause.
//HAVING clause.
//SELECT clause.
//DISTINCT clause.
//ORDER BY clause.
//LIMIT and / or OFFSET clause.
            StringBuilder sb = new StringBuilder();
            sb
                .Append("FROM ")
                .Append( _from is SqlSelect ? $"({_from.ToString()})" : _from.ToString())
                .AppendLine()
                .Append("WHERE ")
                .Append(_where.ToString())
                .AppendLine()
                .Append("SELECT ")
                .Append(string.Join(",",_selects))
                .AppendLine()
                .Append("ORDER BY ")
                .Append(string.Join(",", _orders))
                .AppendLine()
                .Append("LIMIT ")
                .Append(_limit.ToString())
                .AppendLine()
                .Append("OFFSET ")
                .Append(_offset.ToString());
            return sb.ToString();
        }
    }
}

