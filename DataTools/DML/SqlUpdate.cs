using DataTools.Extensions;
using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlUpdate : SqlExpression
    {
        private static readonly SqlWhereClause _whereDummy = new SqlWhereClause(new SqlConstant(1)).Eq(new SqlConstant(1));

        protected SqlWhereClause _where = _whereDummy;
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
    }
}

