using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlFunction : ISqlExpression
    {
        private static readonly ISqlExpression[] _emptyParameters = new ISqlExpression[0];

        private string _functionName;
        private IEnumerable<ISqlExpression> _parameters = _emptyParameters;

        public string FunctionName => _functionName;
        public IEnumerable<ISqlExpression> Parameters => _parameters;

        public SqlFunction(string functionName, params ISqlExpression[] parameters)
        {
            _functionName = functionName;
            _parameters = parameters;
        }

        public SqlFunction() { }

        public SqlFunction Call(string functionName)
        {
            _functionName = functionName;
            return this;
        }

        public SqlFunction Parameter(params ISqlExpression[] parameters)
        {
            _parameters = parameters;
            return this;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlFunction sqlFunction)
            {
                if (_functionName != sqlFunction._functionName) return false;

                var leftE = _parameters.GetEnumerator();
                var rightE = sqlFunction._parameters.GetEnumerator();
                while (leftE.MoveNext())
                {
                    if (!rightE.MoveNext()) return false;
                    if (!leftE.Current.Equals(rightE.Current)) return false;
                }
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            return $"{_functionName}({string.Join(",", _parameters)})";
        }
    }
}