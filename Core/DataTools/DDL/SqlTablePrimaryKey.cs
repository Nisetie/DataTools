namespace DataTools.DDL
{
    public class SqlTablePrimaryKey : SqlTableConstraint
    {
        public string[] Columns;

        public SqlTablePrimaryKey(params string[] columns)
        {
            Columns = columns;

            PayloadLength = ToString().Length;
        }
        public override string ToString() => $"PRIMARY KEY ({string.Join(",", Columns)})";
    }

}