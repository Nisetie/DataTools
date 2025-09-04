using DataTools.DML;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlProcedureExtensions
    {
        public static SqlProcedure Parameter(this SqlProcedure sqlProcedure, params object[] parameters)
        {
            return sqlProcedure.Parameter(parameters.Select(p => new SqlConstant(p)).ToArray());
        }
    }
}
