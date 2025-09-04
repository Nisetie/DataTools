using DataTools.Attributes;
using System;

namespace DataTools_Tests
{
    [ObjectName("TestModelExtra", "dbo"), DisplayModelName("Дополнительная тестовая модель")]
    public class TestModelExtra
    {
        [IgnoreChanges, Unique, Autoincrement]
        public int Id { get; set; }
        public string Name { get; set; }
        public int? Value { get; set; }
        public float? FValue { get; set; }
        public double? GValue { get; set; }
        public DateTime? Timestamp { get; set; }
        [Reference(foreignFieldNames: new string[] { nameof(TestModelExtra.Id) }, columnNames: new string[] { "Extra_id" })]
        public TestModelExtra Extra { get; set; }
        public override string ToString()
        {
            return Name;
        }
    }
}

