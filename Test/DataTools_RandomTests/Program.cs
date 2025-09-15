using DataTools.Common;
using DataTools.Deploy;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;

namespace DataTools_RandomTests
{
    internal class Program
    {
        static string _mssqlConnectionString = "Data source=localhost\\sqlexpress;Database=AdventureWorksLT2012;Integrated security=true;Trust server certificate = true";
        static void Main(string[] args)
        {
            var ctx = new DataTools.MSSQL.MSSQL_DataContext();
            ctx.ConnectionString = _mssqlConnectionString;

            var worker = new DataTools.Deploy.GeneratorWorker(new DataTools.Deploy.GeneratorOptions()
            {
                ConnectionString = _mssqlConnectionString,
                DBMS = DataTools.Common.E_DBMS.MSSQL
            });

            var modelDefinitions = worker.GetModelDefinitions().ToArray();

            var modelDef = modelDefinitions.Where(md => md.ModelMetadata.FullObjectName == "SalesLT.SalesOrderDetail").First();

            var mm = modelDef.ModelMetadata;

            var result = ctx.Select(mm).ToArray();

            dynamic dm = new DynamicModel(new ModelMetadata());
            Console.WriteLine(dm.ToString());
            Console.Read();
        }

        void TestReplacer()
        {
            var ctx = new DataTools.MSSQL.MSSQL_DataContext();
            ctx.ConnectionString = _mssqlConnectionString;

            var worker = new DataTools.Deploy.GeneratorWorker(new DataTools.Deploy.GeneratorOptions()
            {
                ConnectionString = _mssqlConnectionString,
                DBMS = DataTools.Common.E_DBMS.MSSQL
            });

            var modelDefinitions = worker.GetModelDefinitions().ToArray();

            var modelDef = modelDefinitions.Where(md => md.ModelMetadata.FullObjectName == "SalesLT.SalesOrderDetail").First();

            var mm = modelDef.ModelMetadata;

            ctx.Execute(new SqlCustom(@"
drop function if exists SalesLT.SalesOrderDetail_Select;
create function SalesLT.SalesOrderDetail_Select()
returns table
as 
return (select * from SalesLT.SalesOrderDetail);
"));

            var cf = CRUDReplacer.CreateCallFunction(mm, "SalesLT.SalesOrderDetail_Select");
        }
    }

    public class CRUDReplacer
    {
        //IModelMetadata Metadata { get; }
        //Func<ModelT, SqlExpression> _select, _insert, _delete, _update;

        //public CRUDReplacer(
        //    IModelMetadata modelMetadata,
        //    Func<ModelT, SqlExpression> select,
        //    Func<ModelT, SqlExpression> insert,
        //    Func<ModelT, SqlExpression> update,
        //    Func<ModelT, SqlExpression> delete
        //    )
        //{
        //    Metadata = modelMetadata;            
        //}

        public static ISqlExpression CreateCallFunction<ModelT>(string functionName) where ModelT : class, new()
        {
            return CreateCallFunction(ModelMetadata<ModelT>.Instance, functionName);
        }

        public static ISqlExpression CreateCallFunction(IModelMetadata meta, string functionName)
        {
            List<SqlParameter> parameters = new List<SqlParameter>();
            foreach (var f in meta.GetFilterableFields())
            {
                if (f.IsForeignKey)
                {
                    foreach (var cn in f.ColumnNames)
                        parameters.Add(new SqlParameter(cn));
                }
                else
                    parameters.Add(new SqlParameter(f.ColumnName));
            }
            if (parameters.Count == 0)
                foreach (var f in meta.Fields)
                {
                    if (f.IsForeignKey)
                    {
                        foreach (var cn in f.ColumnNames)
                            parameters.Add(new SqlParameter(cn));
                    }
                    else
                        parameters.Add(new SqlParameter(f.ColumnName));
                }
            return new SqlFunction(functionName, parameters.ToArray());
        }

        public static ISqlExpression CreateCallProcedure<ModelT>(string procedureName) where ModelT : class, new()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            List<SqlParameter> parameters = new List<SqlParameter>();
            foreach (var f in meta.GetFilterableFields())
            {
                if (f.IsForeignKey)
                {
                    foreach (var cn in f.ColumnNames)
                        parameters.Add(new SqlParameter(cn));
                }
                else
                    parameters.Add(new SqlParameter(f.ColumnName));
            }
            if (parameters.Count == 0)
                foreach (var f in meta.Fields)
                {
                    if (f.IsForeignKey)
                    {
                        foreach (var cn in f.ColumnNames)
                            parameters.Add(new SqlParameter(cn));
                    }
                    else
                        parameters.Add(new SqlParameter(f.ColumnName));
                }
            return new SqlProcedure(procedureName, parameters.ToArray());
        }
    }
}