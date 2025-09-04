using DataTools.Attributes;
using System;

namespace DataTools_Tests
{
    [ObjectName("TestModelGuidChild", "dbo")]
    public class TestModelGuidChild
    {
        [Unique]
        public Guid Id { get; set; }
        public string Name { get; set; }
        [Reference(foreignFieldNames: new string[] { nameof(TestModelGuidParent.Id) }, columnNames: new string[] { "parent_id" })]
        public TestModelGuidParent Parent { get; set; }
    }
}

