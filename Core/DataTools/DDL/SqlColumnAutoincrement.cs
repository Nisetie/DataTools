namespace DataTools.DDL
{
    public class SqlColumnAutoincrement : SqlColumnConstraint
    {
        public override string ToString() => "GENERATED AS IDENTITY";
    }

}