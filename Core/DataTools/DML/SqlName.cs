namespace DataTools.DML
{
    public class SqlName : SqlExpression
    {
        private string _name;
        public string Name
        {
            get => _name;
            private set
            {
                _name = value;
                PayloadLength = _name?.Length ?? 0;
            }
        }
        public SqlName(string name) => Name = name.Trim();


        public override string ToString()
        {
            return Name;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlName sqlName)
                return Name == sqlName.Name;
            return false;
        }
    }
}

