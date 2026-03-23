namespace DataTools.DML
{
    public class SqlParameter : SqlExpression
    {
        private string _name;
        private object _value;
        public string Name
        {
            get
            {
                return _name;
            }
            set
            {
                _name = value;
                PayloadLength = ToString().Length;
            }
        }

        public object Value
        {
            get { return _value; }
            set
            {
                _value = value;
            }
        }
        public SqlParameter(string name)
        {
            _name = name;
            PayloadLength = ToString().Length;
        }

        public SqlParameter(string name, object value)
        {
            Name = name;
            Value = value;
        }

        public override string ToString()
        {
            return $"${Name}";
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlParameter sqlName)
                return Name == sqlName.Name;
            return false;
        }
    }
}

