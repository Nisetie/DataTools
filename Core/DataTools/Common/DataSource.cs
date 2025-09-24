using DataTools.DML;
using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.Common
{
    public abstract class DataSource : IDataSource
    {
        public abstract void Execute(ISqlExpression query, params SqlParameter[] parameters);
        public void Execute(ISqlExpression query) => Execute(query, null);
        public abstract object ExecuteScalar(ISqlExpression query, params SqlParameter[] parameters);
        public object ExecuteScalar(ISqlExpression query) => ExecuteScalar(query, null);
        public abstract IEnumerable<object[]> ExecuteWithResult(ISqlExpression query, params SqlParameter[] parameters);
        public IEnumerable<object[]> ExecuteWithResult(ISqlExpression query) => ExecuteWithResult(query);
    }
}
