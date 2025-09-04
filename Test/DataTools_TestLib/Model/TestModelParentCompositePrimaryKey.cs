using DataTools.Attributes;

namespace DataTools_Tests
{
    [ObjectName(nameof(TestModelParentCompositePrimaryKey), "dbo")]
    public class TestModelParentCompositePrimaryKey
    {
        [PrimaryKey]
        public int i { get; set; }
        [PrimaryKey]
        public int j { get; set; }

        public string k { get; set; }
    }
}

