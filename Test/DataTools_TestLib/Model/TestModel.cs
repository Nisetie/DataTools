using DataTools.Attributes;
using System;

namespace DataTools_Tests
{
    [ObjectName("TestModel", "dbo"), DisplayModelName("Тестовая модель")]
    public class TestModel
    {
        [IgnoreChanges, Unique, Autoincrement]
        public int Id { get; set; }
        public long? LongId { get; set; }
        public short? ShortId { get; set; }
        public string Name { get; set; }
        public string CharCode { get; set; }
        public bool Checked { get; set; }
        public int Value { get; set; }
        public float FValue { get; set; }
        public double GValue { get; set; }
        public decimal Money { get; set; }
        public DateTime Timestamp { get; set; }
        public TimeSpan Duration { get; set; }
        public Guid Guid { get; set; }

        public byte[] bindata { get; set; }

        public override string ToString()
        {
            return Name;
        }
    }
}

