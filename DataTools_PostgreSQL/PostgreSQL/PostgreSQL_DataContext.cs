using DataTools.Common;
using DataTools.Interfaces;
using System;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_DataContext : DataContext
    {
        public string ConnectionString { get; set; }

        public PostgreSQL_DataContext() : base() { }

        public PostgreSQL_DataContext(string connectionString) : base()
        {
            this.ConnectionString = connectionString;

            this.AddCustomTypeConverter<DateTimeOffset>(value => new DateTimeOffset((DateTime)value));
        }

        protected override IDataSource _GetDataSource()
        {
            var ds = new PostgreSQL_DataSource(ConnectionString);
            return ds;
        }
    }
}