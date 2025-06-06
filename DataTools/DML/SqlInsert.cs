using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlInsert : SqlExpression
    {
        protected SqlName _into;
        protected IEnumerable<SqlName> _columns;
        protected IEnumerable<SqlExpression> _values;

        public SqlExpression IntoDestination => _into;
        public IEnumerable<SqlName> Columns => _columns;
        public IEnumerable<SqlExpression> Values => _values;

        public SqlInsert Into(SqlName objectName)
        {
            _into = objectName;
            return this;
        }
        public SqlInsert Column(params SqlName[] columns)
        {
            _columns = columns;
            return this;
        }
        public SqlInsert Value(params SqlExpression[] values)
        {
            _values = values;
            return this;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlInsert sqlInsert)
            {
                if (!_into.Equals(sqlInsert._into)) return false;
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
            var sb = new StringBuilder();

            sb
                .AppendLine($"INSERT {_into}")
                .AppendLine($"({string.Join(",", _columns)})")
                .AppendLine($"VALUES ({string.Join(",",_values)})");

            return sb.ToString();
        }
    }
}

