using DataTools.DML;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlFunctionExtensions
    {
        public static SqlFunction Parameter(this SqlFunction sqlFunction, params object[] parameters)
        {
            return sqlFunction.Parameter(parameters.Select(p => new SqlConstant(p)).ToArray());
        }
    }
}
