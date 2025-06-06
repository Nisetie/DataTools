using DataTools.Common;
using DataTools.Interfaces;

namespace DataTools.SQLite
{
    public class SQLite_DataContext : DataContext
    {
        private string _connectionString;

        public string ConnectionString
        {
            get => _connectionString; 
            set => _connectionString = value;
        }

        public SQLite_DataContext() : base() { }

        public SQLite_DataContext(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }

        protected override IDataSource _GetDataSource()
        {
            var ds = new SQLite_DataSource(ConnectionString);
            return ds;
        }
    }
}