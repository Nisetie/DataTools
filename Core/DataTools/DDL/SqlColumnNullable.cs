namespace DataTools.DDL
{
    public class SqlColumnNullable : SqlColumnConstraint
    {
        private bool _isNullable;
        public bool IsNullable => _isNullable;
        public SqlColumnNullable(bool isNullable = true) => _isNullable = isNullable;
        public override string ToString() => _isNullable ? "NULL" : "NOT NULL";
    }

}