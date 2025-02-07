using System.Collections.Generic;

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
    }
}