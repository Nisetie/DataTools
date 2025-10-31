using System;
using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlInsertBatch : SqlExpression
    {
        protected SqlName _into;
        protected IEnumerable<SqlName> _columns;
        protected IEnumerable<IEnumerable<ISqlExpression>> _values;

        public ISqlExpression IntoDestination => _into;
        public IEnumerable<SqlName> Columns => _columns;
        public IEnumerable<IEnumerable<ISqlExpression>> Values => _values;

        public SqlInsertBatch Into(SqlName objectName)
        {
            PayloadLength -= _into?.PayloadLength ?? 0;
            _into = objectName;
            PayloadLength += _into?.PayloadLength ?? 0;
            return this;
        }
        public SqlInsertBatch Column(params SqlName[] columns)
        {
            if (_columns != null) foreach (var c in _columns) PayloadLength -= c?.PayloadLength ?? 0;
            _columns = columns;
            if (_columns != null) foreach (var c in _columns) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }
        public SqlInsertBatch Value(params ISqlExpression[][] values)
        {
            if (_values != null) 
                foreach (var vals in _values) 
                    if (vals != null) 
                        foreach (var v in vals) PayloadLength -= v?.PayloadLength ?? 0;
            _values = values;
            if (_values != null)
                foreach (var vals in _values)
                    if (vals != null)
                        foreach (var v in vals) PayloadLength += v?.PayloadLength ?? 0;
            return this;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlInsertBatch sqlInsert)
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
                {
                    if (!rightVE.MoveNext()) return false;
                    var leftVVE = leftVE.Current.GetEnumerator();
                    var rightVVE = rightVE.Current.GetEnumerator();
                    while (leftVVE.MoveNext())
                        if (!rightVVE.MoveNext() || !leftVVE.Current.Equals(rightVVE.Current)) return false;
                }
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            sb
                .AppendLine($"INSERT {_into}")
                .AppendLine($"({string.Join(",", _columns)})")
                .AppendLine("VALUES");
            foreach (var val in _values)
            {
                sb.Append("(");
                foreach (var col in val)
                {
                    sb.Append(col.ToString());
                    sb.Append(",");
                }
                sb.Length -= 1;
                sb.Append($"),{Environment.NewLine}");
            }
            sb.Length -= $",{Environment.NewLine}".Length;

            return sb.ToString();
        }
    }
}

