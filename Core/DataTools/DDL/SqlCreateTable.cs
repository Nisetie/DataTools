using DataTools.Common;
using DataTools.DML;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataTools.DDL
{
    public class SqlCreateTable : ISqlExpression
    {
        public SqlName TableName { get; set; } = null;
        public IEnumerable<SqlColumnDefinition> Columns { get; set; } = null;

        public IEnumerable<SqlTableConstraint> Constraints { get; set; } = null;

        public SqlCreateTable() { }

        public SqlCreateTable Table(SqlName tableName)
        {
            TableName = tableName;
            return this;
        }

        public SqlCreateTable Column(params SqlColumnDefinition[] columns)
        {
            Columns = columns;
            return this;
        }

        public SqlCreateTable Constraint(params SqlTableConstraint[] constraints)
        {
            Constraints = constraints;
            return this;
        }

        public override string ToString()
        {
            var sb = new StringBuilder(128);
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

    public class SqlColumnDefinition
    {
        public SqlName ColumnName { get; set; }
        public DBType ColumnType { get; set; }
        public int? TextLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public IEnumerable<SqlColumnConstraint> Constraints { get; set; }

        public SqlColumnDefinition Name(SqlName name)
        {
            ColumnName = name;
            return this;
        }

        public SqlColumnDefinition Name(string name)
        {
            return Name(new SqlName(name));
        }

        public SqlColumnDefinition Type(DBType type, int? length = null)
        {
            ColumnType = type;
            TextLength = length;
            return this;
        }

        public SqlColumnDefinition Type<T>(int? length = null)
        {
            return Type(DBType.GetDBTypeByType(typeof(T)), length);
        }

        public SqlColumnDefinition Constraint(params SqlColumnConstraint[] constraints)
        {
            Constraints = constraints;
            return this;
        }

        public override string ToString()
        {
            return $"{ColumnName} {ColumnType} {(Constraints != null ? string.Join(" ", Constraints.Select(c => c.ToString())) : "")}";
        }
    }

    public abstract class SqlColumnConstraint : ISqlExpression { }

    public abstract class SqlTableConstraint : ISqlExpression { }

    public class SqlColumnNullable : SqlColumnConstraint
    {
        private bool _isNullable;
        public bool IsNullable => _isNullable;
        public SqlColumnNullable(bool isNullable = true) => _isNullable = isNullable;
        public override string ToString() => _isNullable ? "NULL" : "NOT NULL";
    }

    public class SqlTablePrimaryKey : SqlTableConstraint
    {
        public string[] Columns;

        public SqlTablePrimaryKey(params string[] columns)
        {
            Columns = columns;
        }
        public override string ToString() => $"PRIMARY KEY ({string.Join(",", Columns)})";
    }

    public class SqlTableForeignKey : SqlTableConstraint
    {
        public string[] Columns;
        public string ForeignTableName;
        public string[] ForeignColumns;

        public SqlTableForeignKey(string foreignTableName, string[] columns, string[] foreignColumns)
        {
            Columns = columns;
            ForeignColumns = foreignColumns;
            ForeignTableName = foreignTableName;
        }
        public override string ToString() => $"FOREIGN KEY ({string.Join(",", Columns)}) REFERENCES {ForeignTableName}({string.Join(",", ForeignColumns)})";
    }

    public class SqlTableUnique : SqlTableConstraint
    {
        public string[] Columns;

        public SqlTableUnique(string[] columns)
        {
            Columns = columns;
        }
        public override string ToString() => $"UNIQUE ({string.Join(",", Columns)})";
    }

    public class SqlColumnAutoincrement : SqlColumnConstraint
    {
        public override string ToString() => "GENERATED AS IDENTITY";
    }

}