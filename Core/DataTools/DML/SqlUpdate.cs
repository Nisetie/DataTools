using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlUpdate : SqlExpression
    {
        protected SqlWhere _where;
        protected ISqlExpression _from;
        protected IEnumerable<SqlName> _columns;
        protected IEnumerable<ISqlExpression> _values;

        public ISqlExpression FromSource => _from;
        public IEnumerable<SqlName> Columns => _columns;
        public IEnumerable<ISqlExpression> Values => _values;
        public SqlWhere Wheres => _where;

        public SqlUpdate From(SqlName objectName)
        {
            PayloadLength -= _from?.PayloadLength ?? 0;
            _from = objectName;
            PayloadLength += _from?.PayloadLength ?? 0;
            return this;
        }
        public SqlUpdate Set(params SqlName[] columns)
        {
            if (_columns != null) foreach (var c in _columns) PayloadLength -= c?.PayloadLength ?? 0;
            _columns = columns;
            if (_columns != null) foreach (var c in _columns) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }

        public SqlUpdate Value(params ISqlExpression[] values)
        {
            if (_values != null) foreach (var c in _values) PayloadLength -= c?.PayloadLength ?? 0;
            _values = values;
            if (_values != null) foreach (var c in _values) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }

        public SqlUpdate Where(SqlWhere where)
        {
            PayloadLength -= _where?.PayloadLength ?? 0;
            _where = where;
            PayloadLength += _where?.PayloadLength ?? 0;
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

