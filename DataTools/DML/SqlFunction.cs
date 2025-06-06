using System.Collections.Generic;
using System.Linq;

namespace DataTools.DML
{
    public class SqlFunction : SqlExpression
    {
        private static readonly SqlExpression[] _emptyParameters = new SqlExpression[0];

        private string _functionName;
        private IEnumerable<SqlExpression> _parameters = _emptyParameters;

        public string FunctionName => _functionName;
        public IEnumerable<SqlExpression> Parameters => _parameters;

        public SqlFunction(string functionName, IEnumerable<SqlExpression> parameters)
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

        public SqlFunction Parameter(params SqlExpression[] parameters)
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
            return $"{_functionName}({string.Join(",",_parameters)})";
        }
    }
}