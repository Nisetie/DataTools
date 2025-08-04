using DataTools.DML;

namespace DataTools.DDL
{
    public class SqlDropTable : SqlExpression
    {
        public SqlName TableName { get; private set; }

        public SqlDropTable Table(SqlName tableName) { TableName = tableName; return this; }

        public override string ToString()
        {
            return $"DROP TABLE {TableName}";
        }

        public override bool Equals(object obj)
        {
            return obj is SqlDropTable other && TableName.Equals(other.TableName);
        }
    }
}