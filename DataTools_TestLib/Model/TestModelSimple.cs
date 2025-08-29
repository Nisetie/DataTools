using DataTools.Attributes;

namespace DataTools_Tests
{
    [ObjectName("TestModelNoUnique", "dbo")]
    [NoUniqueAttribute]
    public class TestModelSimple
    {
        //[Unique]
        public int? Id { get; set; }
        public string Name { get; set; }
    }
}

