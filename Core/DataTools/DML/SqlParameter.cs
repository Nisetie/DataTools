namespace DataTools.DML
{
    public class SqlParameter : ISqlExpression
    {
        private string _name;
        private object _value;
        public string Name => _name;
        public object Value
        {
            get { return _value; }
            set { _value = value; }
        }
        public SqlParameter(string name) => _name = name;
        public SqlParameter(string name, object value)
        {
            _name = name;
            _value = value;
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

