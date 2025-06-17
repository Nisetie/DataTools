using DataTools.Extensions;
using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlUpdate : SqlExpression
    {
        protected SqlWhereClause _where;
        protected SqlExpression _from;
        protected IEnumerable<SqlName> _columns;
        protected IEnumerable<SqlExpression> _values;

        public SqlExpression FromSource => _from;
        public IEnumerable<SqlName> Columns => _columns;
        public IEnumerable<SqlExpression> Values => _values;
        public SqlWhereClause Wheres => _where;

        public SqlUpdate From(SqlName objectName)
        {
            _from = objectName;
            return this;
        }
        public SqlUpdate Set(params SqlName[] columns)
        {
            _columns = columns;
            return this;
        }

        public SqlUpdate Value(params SqlExpression[] values)
        {
            _values = values;
            return this;
        }

        public SqlUpdate Where(SqlWhereClause where)
        {
            _where = where;
            return this;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlUpdate sqlUpdate)
            {
                if (!(_from == null ? sqlUpdate._from == null : _from.Equals(sqlUpdate._from))
                    || !(_where == null ? sqlUpdate._where == null : _where.Equals(sqlUpdate._where)))
                    return false;
                if ((_columns == null && sqlUpdate._columns != null) || (_columns != null && sqlUpdate._columns == null)) return false;
                var leftCE = _columns.GetEnumerator();
                var rightCE = sqlUpdate._columns.GetEnumerator();
                while (leftCE.MoveNext())
                    if (!rightCE.MoveNext() || !leftCE.Current.Equals(rightCE.Current)) return false;
                var leftVE = _values.GetEnumerator();
                var rightVE = sqlUpdate._values.GetEnumerator();
                while (leftVE.MoveNext())
                    if (!rightVE.MoveNext() || !leftVE.Current.Equals(rightVE.Current)) return false;
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            sb
                .AppendLine($"UPDATE {_from}")
                .AppendLine($"SET")
                .AppendLine($"{string.Join(",", _columns)}")
                .AppendLine("=")
                .AppendLine($"{string.Join(",", _values)}");
            if (_where != null)
                sb.AppendLine($"WHERE {_where}");

            return sb.ToString();
        }
    }
}

