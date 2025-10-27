using DataTools.Common;
using DataTools.DML;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.DDL
{
    public class SqlDDLColumnDefinition : ISqlExpression
    {
        public SqlName ColumnName { get; set; }
        public DBType ColumnType { get; set; }
        public int? TextLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public IEnumerable<SqlColumnConstraint> Constraints { get; set; }

        public SqlDDLColumnDefinition Name(SqlName name)
        {
            ColumnName = name;
            return this;
        }

        public SqlDDLColumnDefinition Name(string name)
        {
            return Name(new SqlName(name));
        }

        public SqlDDLColumnDefinition Type(DBType type, int? length = null)
        {
            ColumnType = type;
            TextLength = length;
            return this;
        }

        public SqlDDLColumnDefinition Type<T>(int? length = null)
        {
            return Type(DBType.GetDBTypeByType(typeof(T)), length);
        }

        public SqlDDLColumnDefinition Constraint(params SqlColumnConstraint[] constraints)
        {
            Constraints = constraints;
            return this;
        }

        public override string ToString()
        {
            return $"{ColumnName} {ColumnType} {(Constraints != null ? string.Join(" ", Constraints.Select(c => c.ToString())) : "")}";
        }
    }

}