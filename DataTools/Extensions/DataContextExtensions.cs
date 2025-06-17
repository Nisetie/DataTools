using DataTools.Commands;
using DataTools.DML;
using DataTools.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Extensions
{
    public static class DataContextExtensions
    {
        public static SelectCommmand<ModelT> SelectFrom<ModelT>(this IDataContext context) where ModelT : class, new()
        {
            return new SelectCommmand<ModelT>(context);
        }

        public static IEnumerable<ModelT> CallProcedure<ModelT>(this IDataContext context, string pName, params object[] pars)
            where ModelT : class, new()
        {
            var p = new SqlProcedure()
                .Call(pName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray());
            return context.CallProcedure<ModelT>(p);
        }

        public static void CallProcedure(this IDataContext context, string pName, params object[] pars)
        {
            var p = new SqlProcedure()
                .Call(pName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray());
            context.CallProcedure(p);
        }

        public static IEnumerable<ModelT> CallTableFunction<ModelT>(this IDataContext context, string fName, params object[] pars)
            where ModelT : class, new()
        {
            var f = new SqlFunction()
                .Call(fName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray());
            return context.CallTableFunction<ModelT>(f);
        }

        public static object CallScalarFunction(this IDataContext context, string fName, params object[] pars)
        {
            var f = new SqlFunction()
                .Call(fName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray());
            return context.CallScalarFunction(f);
        }
    }
}