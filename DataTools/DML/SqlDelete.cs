using System.Text;

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

        public override bool Equals(object obj)
        {
            return obj is SqlDelete sqlDelete
                && _from.Equals(sqlDelete._from)
                && _where.Equals(sqlDelete._where);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb
                .AppendLine($"DELETE {_from}")
                .AppendLine($"WHERE {_where}");
            return sb.ToString();
        }
    }
}

