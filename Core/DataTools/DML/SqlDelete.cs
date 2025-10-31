using System.Text;

namespace DataTools.DML
{
    public class SqlDelete : SqlExpression
    {
        protected ISqlExpression _from;
        protected SqlWhere _where;

        public ISqlExpression FromSource => _from;
        public SqlWhere Wheres => _where;

        public SqlDelete From(SqlName objectName)
        {
            PayloadLength -= _from?.PayloadLength ?? 0;
            _from = objectName;
            PayloadLength += _from?.PayloadLength ?? 0;
            return this;
        }

        public SqlDelete Where(SqlWhere where)
        {
            PayloadLength -= _where?.PayloadLength ?? 0;
            _where = where;
            PayloadLength += _where?.PayloadLength ?? 0;
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

