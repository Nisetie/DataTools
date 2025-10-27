namespace DataTools.DDL
{
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

}