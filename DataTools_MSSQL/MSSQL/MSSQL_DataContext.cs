using DataTools.Common;
using DataTools.Interfaces;

namespace DataTools.MSSQL
{
    public class MSSQL_DataContext : DataContext
    {
        public string ConnectionString { get; set; }

        public MSSQL_DataContext() : base() { }

        public MSSQL_DataContext(string connectionString) : this()
        {
            this.ConnectionString = connectionString;
        }

        protected override IDataSource _GetDataSource()
        {
            var ds = new MSSQL_DataSource();
            ds.Initialize(this);
            return ds;
        }
    }
}