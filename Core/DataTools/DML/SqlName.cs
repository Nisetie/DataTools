namespace DataTools.DML
{
    public class SqlName : ISqlExpression
    {
        private string _name;
        public string Name => _name;
        public SqlName(string name) => _name = name.Trim();

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

