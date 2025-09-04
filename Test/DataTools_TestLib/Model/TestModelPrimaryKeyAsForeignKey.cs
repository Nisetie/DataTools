using DataTools.Attributes;

namespace DataTools_Tests
{
    [ObjectName(nameof(TestModelPrimaryKeyAsForeignKey), "dbo")]
    public class TestModelPrimaryKeyAsForeignKey
    {
        [PrimaryKey]
        [Reference(foreignFieldNames: new string[] { nameof(TestModelChildCompositePrimaryKey.i), nameof(TestModelChildCompositePrimaryKey.j) }, columnNames: new string[] { "Child_i", "Child_j" })]
        public TestModelChildCompositePrimaryKey Child { get; set; }

        public string k { get; set; }
    }
}

