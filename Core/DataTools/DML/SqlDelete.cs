using System.Text;

namespace DataTools.DML
{
    public class SqlDelete : ISqlExpression
    {
        protected ISqlExpression _from;
        protected SqlWhere _where;

        public ISqlExpression FromSource => _from;
        public SqlWhere Wheres => _where;

        public SqlDelete From(SqlName objectName)
        {
            _from = objectName;
            return this;
        }

        public SqlDelete Where(SqlWhere where)
        {
            _where = where;
            return this;
        }

        public override bool Equals(object obj)
        {
            return obj is SqlDelete sqlDelete
                && (_from == null ? sqlDelete._from == null : _from.Equals(sqlDelete._from))
                && (_where == null ? sqlDelete._where == null : _where.Equals(sqlDelete._where));
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"DELETE {_from}");
            if (_where != null)
                sb.AppendLine($"WHERE {_where}");
            return sb.ToString();
        }
    }
}

