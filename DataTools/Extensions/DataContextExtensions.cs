using DataTools.Commands;
using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Extensions
{
    public static class DataContextExtensions
    {
        public static SelectCommand<ModelT> SelectFrom<ModelT>(this IDataContext context) where ModelT : class, new() => new SelectCommand<ModelT>(context);

        public static SelectCommand SelectFrom(this IDataContext context, IModelMetadata metadata) => new SelectCommand(context, metadata);

        public static void CreateTable(this IDataContext context, IModelMetadata modelMetadata) => context.Execute(new SqlCreateTable().Table(modelMetadata));
        public static void CreateTable<ModelT>(this IDataContext context) where ModelT : class, new() => CreateTable(context, ModelMetadata<ModelT>.Instance);
        public static void DropTable(this IDataContext context, IModelMetadata modelMetadata) => context.Execute(new SqlDropTable().Table(modelMetadata));
        public static void DropTable<ModelT>(this IDataContext context) where ModelT : class, new() => DropTable(context, ModelMetadata<ModelT>.Instance);

        public static IEnumerable<ModelT> CallProcedure<ModelT>(this IDataContext context, string pName, params object[] pars)
            where ModelT : class, new()
            => context.CallProcedure<ModelT>(new SqlProcedure()
                .Call(pName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray()));
        public static IEnumerable<dynamic> CallProcedure(this IDataContext context, IModelMetadata modelMetadata, string pName, params object[] pars)
            => context.CallProcedure(modelMetadata, new SqlProcedure()
                .Call(pName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray()));

        public static void CallProcedure(this IDataContext context, string pName, params object[] pars)
            => context.CallProcedure(new SqlProcedure()
                .Call(pName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray()));

        public static IEnumerable<ModelT> CallTableFunction<ModelT>(this IDataContext context, string fName, params object[] pars)
            where ModelT : class, new()
            => context.CallTableFunction<ModelT>(new SqlFunction()
                .Call(fName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray()));
        public static IEnumerable<dynamic> CallTableFunction(this IDataContext context, IModelMetadata modelMetadata, string fName, params object[] pars)
            => context.CallTableFunction(modelMetadata, new SqlFunction()
                .Call(fName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray()));

        public static object CallScalarFunction(this IDataContext context, string fName, params object[] pars)
            => context.CallScalarFunction(new SqlFunction()
                .Call(fName)
                .Parameter((from par in pars select new SqlConstant(par)).ToArray()));
    }
}