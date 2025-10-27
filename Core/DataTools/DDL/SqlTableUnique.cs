namespace DataTools.DDL
{
    public class SqlTableUnique : SqlTableConstraint
    {
        public string[] Columns;

        public SqlTableUnique(string[] columns)
        {
            Columns = columns;
        }
        public override string ToString() => $"UNIQUE ({string.Join(",", Columns)})";
    }

}