using DataTools.Common;
using DataTools.Deploy;
using DataTools.Extensions;
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

            var result = ctx.SelectFrom(mm).Select().ToArray();

            dynamic dm = new DynamicModel(new ModelMetadata());
            Console.WriteLine(dm.ToString());
            Console.Read();
        }
    }
}
