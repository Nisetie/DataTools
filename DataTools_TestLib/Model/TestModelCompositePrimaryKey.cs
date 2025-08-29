using DataTools.Attributes;

namespace DataTools_Tests
{
    [ObjectName(nameof(TestModelCompositePrimaryKey), "dbo")]
    public class TestModelCompositePrimaryKey
    {
        [PrimaryKey]
        public int i { get; set; }
        [PrimaryKey]
        public int j { get; set; }
        public string k { get; set; }
    }
}

