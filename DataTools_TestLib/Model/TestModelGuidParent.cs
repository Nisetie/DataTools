using DataTools.Attributes;
using System;

namespace DataTools_Tests
{
    [ObjectName("TestModelGuidParent", "dbo")]
    public class TestModelGuidParent
    {
        [Unique]
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
}

