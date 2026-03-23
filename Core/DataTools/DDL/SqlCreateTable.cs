using DataTools.DML;
using System.Collections.Generic;
using System.Text;

namespace DataTools.DDL
{
    public class SqlCreateTable : SqlExpression
    {
        public SqlName TableName { get; set; } = null;
        public IEnumerable<SqlDDLColumnDefinition> Columns { get; set; } = null;

        public IEnumerable<SqlTableConstraint> Constraints { get; set; } = null;

        public SqlCreateTable() { }

        public SqlCreateTable Table(SqlName tableName)
        {
            PayloadLength -= TableName?.PayloadLength ?? 0;
            TableName = tableName;
            PayloadLength += TableName?.PayloadLength ?? 0;
            return this;
        }

        public SqlCreateTable Column(params SqlDDLColumnDefinition[] columns)
        {
            if (Columns != null) foreach (var c in Columns) PayloadLength -= c?.PayloadLength ?? 0;
            Columns = columns;
            if (Columns != null) foreach (var c in Columns) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }

        public SqlCreateTable Constraint(params SqlTableConstraint[] constraints)
        {
            if (Constraints != null) foreach (var c in Constraints) PayloadLength -= c?.PayloadLength ?? 0;
            Constraints = constraints;
            if (Constraints != null) foreach (var c in Constraints) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }

        public override string ToString()
        {
            var sb = new StringBuilder(256);
            sb.AppendLine($"CREATE TABLE {TableName} (")
                .AppendLine($"{(Columns != null ? string.Join(",", Columns) : "")}")
                .AppendLine($"{(Constraints != null ? "," + string.Join(",", Constraints) : "")})");
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlCreateTable other)) return false;
            if (!(TableName == other.TableName)) return false;

            var leftCols = Columns.GetEnumerator();
            var rightCols = other.Columns.GetEnumerator();
            while (leftCols.MoveNext())
            {
                if (!rightCols.MoveNext()) return false;
                if (!leftCols.Current.Equals(rightCols.Current)) return false;
            }
            return true;
        }
    }
}