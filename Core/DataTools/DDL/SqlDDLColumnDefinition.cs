using DataTools.Common;
using DataTools.DML;
using System.Collections.Generic;

namespace DataTools.DDL
{
    public class SqlDDLColumnDefinition : SqlExpression
    {
        public SqlName ColumnName { get; set; }
        public DBType ColumnType { get; set; }
        public int? TextLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public IEnumerable<SqlColumnConstraint> Constraints { get; set; }

        public SqlDDLColumnDefinition Name(string name)
        {
            return Name(new SqlName(name));
        }

        public SqlDDLColumnDefinition Name(SqlName name)
        {
            PayloadLength -= ColumnName?.PayloadLength ?? 0;
            ColumnName = name;
            PayloadLength += ColumnName?.PayloadLength ?? 0;
            return this;
        }

        public SqlDDLColumnDefinition Type(DBType type, int? length = null)
        {
            PayloadLength -= ColumnType?.ToString().Length ?? 0;
            PayloadLength -= TextLength?.ToString().Length ?? 0;
            ColumnType = type;
            TextLength = length;
            PayloadLength += ColumnType?.ToString().Length ?? 0;
            PayloadLength += TextLength?.ToString().Length ?? 0;
            return this;
        }

        public SqlDDLColumnDefinition Type<T>(int? length = null)
        {
            return Type(DBType.GetDBTypeByType(typeof(T)), length);
        }

        public SqlDDLColumnDefinition Constraint(params SqlColumnConstraint[] constraints)
        {
            if (Constraints != null) foreach (var c in Constraints) PayloadLength -= c?.PayloadLength ?? 0;
            Constraints = constraints;
            if (Constraints != null) foreach (var c in Constraints) PayloadLength += c?.PayloadLength ?? 0;
            return this;
        }

        public override string ToString()
        {
            return $"{ColumnName} {ColumnType} {(Constraints != null ? string.Join(" ", Constraints) : "")}";
        }
    }

}