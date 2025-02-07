using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlProcedure : SqlExpression
    {
        private static readonly SqlExpression[] _emptyParameters = new SqlExpression[0];

        private string _procedureName;
        private IEnumerable<SqlExpression> _parameters = _emptyParameters;

        public string ProcedureName => _procedureName;
        public IEnumerable<SqlExpression> Parameters => _parameters;

        public SqlProcedure(string procedureName, params SqlExpression[] parameters)
        {
            _procedureName = procedureName;
            _parameters = parameters;
        }

        public SqlProcedure() { }

        public SqlProcedure Call(string functionName)
        {
            _procedureName = functionName;
            return this;
        }

        public SqlProcedure Parameter(params SqlExpression[] parameters)
        {
            _parameters = parameters;
            return this;
        }
    }
}