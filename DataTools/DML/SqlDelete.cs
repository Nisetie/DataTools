namespace DataTools.DML
{
    public class SqlDelete : SqlExpression
    {
        private static readonly SqlWhereClause _whereDummy = new SqlWhereClause();

        protected SqlExpression _from;
        protected SqlWhereClause _where = _whereDummy;

        public SqlExpression FromSource => _from;
        public SqlWhereClause Wheres => _where;

        public SqlDelete From(SqlName objectName)
        {
            _from = objectName;
            return this;
        }

        public SqlDelete Where(SqlWhereClause where)
        {
            _where = where;
            return this;
        }
    }
}

