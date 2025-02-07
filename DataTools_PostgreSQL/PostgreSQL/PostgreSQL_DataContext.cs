using DataTools.Common;
using DataTools.Interfaces;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_DataContext : DataContext
    {
        public string ConnectionString { get; set; }

        public PostgreSQL_DataContext() : base() { }

        public PostgreSQL_DataContext(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        protected override IDataSource _GetDataSource()
        {
            var ds = new PostgreSQL_DataSource();
            ds.Initialize(this);
            return ds;
        }
    }
}