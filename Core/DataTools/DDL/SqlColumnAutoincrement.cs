namespace DataTools.DDL
{
    public class SqlColumnAutoincrement : SqlColumnConstraint
    {
        public SqlColumnAutoincrement()
        {
            PayloadLength = ToString().Length;
        }
        public override string ToString() => "GENERATED AS IDENTITY";
    }

}