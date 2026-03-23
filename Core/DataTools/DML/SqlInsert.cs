using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlInsert : SqlExpression
    {
        protected SqlName _into;
        protected IEnumerable<SqlName> _columns;
        protected IEnumerable<ISqlExpression> _values;

        public ISqlExpression IntoDestination => _into;
        public IEnumerable<SqlName> Columns => _columns;
        public IEnumerable<ISqlExpression> Values => _values;

        public SqlInsert Into(SqlName objectName)
        {
            PayloadLength -= _into?.PayloadLength ?? 0;
            _into = objectName;
            PayloadLength += _into?.PayloadLength ?? 0;
            return this;
        }
        public SqlInsert Column(params SqlName[] columns)
        {
            if (_columns != null) foreach (var c in _columns) PayloadLength -= c?.PayloadLength ?? 0;
            _columns = columns;
            if (_columns != null) foreach (var c in _columns) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }
        public SqlInsert Value(params ISqlExpression[] values)
        {
            if (_values != null) foreach (var c in _values) PayloadLength -= c?.PayloadLength ?? 0;
            _values = values;
            if (_values != null) foreach (var c in _values) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlInsert sqlInsert)
            {
                if (_into == null ? sqlInsert._into != null : !_into.Equals(sqlInsert._into)) return false;
                if ((_columns == null && sqlInsert._columns != null) || (_columns != null && sqlInsert._columns == null)) return false;
                var leftCE = _columns.GetEnumerator();
                var rightCE = sqlInsert._columns.GetEnumerator();
                while (leftCE.MoveNext())
                    if (!rightCE.MoveNext() || !leftCE.Current.Equals(rightCE.Current)) return false;
                var leftVE = _values.GetEnumerator();
                var rightVE = sqlInsert._values.GetEnumerator();
                while (leftVE.MoveNext())
                    if (!rightVE.MoveNext() || !leftVE.Current.Equals(rightVE.Current)) return false;
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            var sb = new StringBuilder(256);

            sb
                .AppendLine($"INSERT {_into}")
                .AppendLine($"({string.Join(",", _columns)})")
                .AppendLine($"VALUES ({string.Join(",", _values)})");

            return sb.ToString();
        }
    }
}

