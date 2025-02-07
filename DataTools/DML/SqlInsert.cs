using System.Collections.Generic;

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
    }
}

