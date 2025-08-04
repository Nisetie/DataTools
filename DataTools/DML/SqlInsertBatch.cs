using System;
using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlInsertBatch : SqlExpression
    {
        protected SqlName _into;
        protected IEnumerable<SqlName> _columns;
        protected IEnumerable<IEnumerable<SqlExpression>> _values;

        public SqlExpression IntoDestination => _into;
        public IEnumerable<SqlName> Columns => _columns;
        public IEnumerable<IEnumerable<SqlExpression>> Values => _values;

        public SqlInsertBatch Into(SqlName objectName)
        {
            _into = objectName;
            return this;
        }
        public SqlInsertBatch Column(params SqlName[] columns)
        {
            _columns = columns;
            return this;
        }
        public SqlInsertBatch Value(params SqlExpression[][] values)
        {
            _values = values;
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
                sb.AppendLine($"{string.Join($",{Environment.NewLine}", $"({string.Join(",", val)})")}");

            return sb.ToString();
        }
    }
}

