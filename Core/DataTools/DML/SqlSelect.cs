﻿using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlSelect : ISqlSelect<SqlSelect>
    {
        protected ISqlExpression _from;
        protected IEnumerable<ISqlExpression> _selects;
        protected SqlWhere _where;
        protected IEnumerable<SqlOrderByClause> _orders;
        protected ISqlExpression _offset;
        protected ISqlExpression _limit;

        public IEnumerable<ISqlExpression> Selects => _selects;
        public ISqlExpression FromSource => _from;
        public SqlWhere Wheres => _where;
        public IEnumerable<SqlOrderByClause> Orders => _orders;
        public ISqlExpression OffsetRows => _offset;
        public ISqlExpression LimitRows => _limit;

        public SqlSelect Select(params ISqlExpression[] selects)
        {
            _selects = selects;
            return this;
        }
        public SqlSelect From(ISqlExpression objectName)
        {
            _from = objectName;
            return this;
        }
        public SqlSelect Where(SqlWhere where)
        {
            _where = where;
            return this;
        }
        public SqlSelect OrderBy(params SqlOrderByClause[] order)
        {
            _orders = order;
            return this;
        }
        public SqlSelect Offset(ISqlExpression offset)
        {
            _offset = offset;
            return this;
        }
        public SqlSelect Limit(ISqlExpression limit)
        {
            _limit = limit;
            return this;
        }
        public override bool Equals(object obj)
        {
            if (obj is SqlSelect sqlSelect)
            {
                if (!(_from == null ? sqlSelect._from == null : _from.Equals(sqlSelect._from))
                    || !(_where == null ? sqlSelect._where == null : _where.Equals(sqlSelect._where))
                    || !(_offset == null ? sqlSelect._offset == null : _offset.Equals(sqlSelect._offset))
                    || !(_limit == null ? sqlSelect._limit == null : _limit.Equals(sqlSelect._limit)))
                    return false;
                if ((_selects == null && sqlSelect._selects != null) || (_selects != null && sqlSelect._selects == null)) return false;
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
            if (_from != null)
                sb
                    .Append("FROM ")
                    .Append(_from is SqlSelect ? $"({_from})" : _from.ToString())
                    .AppendLine();
            if (_where != null)
                sb
                    .Append("WHERE ")
                    .Append(_where.ToString())
                    .AppendLine();
            if (_selects != null)
                sb
                    .Append("SELECT ")
                    .Append(string.Join(",", _selects))
                    .AppendLine();
            if (_orders != null)
                sb
                    .Append("ORDER BY ")
                    .Append(string.Join(",", _orders))
                    .AppendLine();
            if (_limit != null)
                sb
                    .Append("LIMIT ")
                    .Append(_limit.ToString())
                    .AppendLine();

            if (_offset != null)
                sb
                    .Append("OFFSET ")
                    .Append(_offset.ToString());
            return sb.ToString();
        }
    }
}

