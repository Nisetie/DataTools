namespace DataTools.DDL
{
    public class SqlTablePrimaryKey : SqlTableConstraint
    {
        public string[] Columns;

        public SqlTablePrimaryKey(params string[] columns)
        {
            Columns = columns;
        }
        public override string ToString() => $"PRIMARY KEY ({string.Join(",", Columns)})";
    }

}