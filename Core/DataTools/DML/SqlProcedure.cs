using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlProcedure : SqlExpression
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
            PayloadLength -= _procedureName?.Length ?? 0;
            _procedureName = functionName;
            PayloadLength += _procedureName?.Length ?? 0;
            return this;
        }

        public SqlProcedure Parameter(params ISqlExpression[] parameters)
        {
            if (_parameters != null) foreach (var p in _parameters) PayloadLength -= p?.PayloadLength ?? 0;
            _parameters = parameters;
            if (_parameters != null) foreach (var p in _parameters) PayloadLength += p?.PayloadLength ?? 0;
            return this;
        }

        public override string ToString()
        {
            return $"CALL {_procedureName}({(_parameters != null ? string.Join(",", _parameters) : string.Empty)})";
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