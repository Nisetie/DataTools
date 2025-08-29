using DataTools.Attributes;

namespace DataTools_Tests
{
    [ObjectName(nameof(TestModelChildCompositePrimaryKey), "dbo")]
    public class TestModelChildCompositePrimaryKey
    {
        [PrimaryKey]
        public int i { get; set; }
        [PrimaryKey]
        public int j { get; set; }

        [Reference(foreignFieldNames: new string[] { nameof(TestModelParentCompositePrimaryKey.i), nameof(TestModelParentCompositePrimaryKey.j) }, columnNames: new string[] { "Parent_i", "Parent_j" })]
        public TestModelParentCompositePrimaryKey Parent { get; set; }

        public string k { get; set; }
    }
}

