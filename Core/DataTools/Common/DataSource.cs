using DataTools.DML;
using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.Common
{
    public abstract class DataSource : IDataSource
    {
        public abstract void Execute(ISqlExpression query, params SqlParameter[] parameters);
        public abstract object ExecuteScalar(ISqlExpression query, params SqlParameter[] parameters);
        public abstract IEnumerable<object[]> ExecuteWithResult(ISqlExpression query, params SqlParameter[] parameters);
    }
}
