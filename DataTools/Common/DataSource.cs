using DataTools.DML;
using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.Common
{
    public abstract class DataSource : IDataSource
    {
        public abstract void Execute(SqlExpression query, params SqlParameter[] parameters);
        public abstract object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters);
        public abstract IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters);
    }
}
