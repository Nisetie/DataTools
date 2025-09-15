using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlProcedure : ISqlExpression
    {
        private static readonly ISqlExpression[] _emptyParameters = new ISqlExpression[0];

        private string _procedureName;
        private IEnumerable<ISqlExpression> _parameters = _emptyParameters;

        public string ProcedureName => _procedureName;
        public IEnumerable<ISqlExpression> Parameters => _parameters;

        public SqlProcedure(string procedureName, params ISqlExpression[] parameters)
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

        public SqlProcedure Parameter(params ISqlExpression[] parameters)
        {
            _parameters = parameters;
            return this;
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlProcedure sqlProcedure)
            {
                if (_procedureName != sqlProcedure._procedureName) return false;
                var leftE = _parameters.GetEnumerator();
                var rightE = sqlProcedure._parameters.GetEnumerator();
                while (leftE.MoveNext())
                    if (!rightE.MoveNext() || !leftE.Current.Equals(rightE.Current)) return false;
                return true;
            }
            return false;
        }
    }
}