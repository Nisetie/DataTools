using DataTools.Common;
using DataTools.Interfaces;
using System.Collections.Generic;
using System.Data.SQLite;

namespace DataTools.SQLite
{
    public class SQLite_DataContext : DataContext
    {
        private List<(string dbName, string schemaName)> _attachedSchemas = new List<(string dbName, string schemaName)>();
        private string _connectionString;

        public string ConnectionString
        {
            get => _connectionString; set
            {

                _connectionString = value;
                // на случай, если БД - это файл с форматом (н-р, mydb.sqlite)
                var connb = new SQLiteConnectionStringBuilder(_connectionString);
                _attachedSchemas.Clear();
                AttachSchema(connb.DataSource, connb.DataSource.Split('.')[0]);
            }
        }

        public IEnumerable<(string dbName, string schemaName)> AttachedSchemas => _attachedSchemas;

        public SQLite_DataContext() : base() { }

        public SQLite_DataContext(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }

        public void AttachSchema(string dbName, string schema) => _attachedSchemas.Add((dbName, schema));
        public void DetachSchema(string dbName, string schema) => _attachedSchemas.Remove((dbName, schema));

        protected override IDataSource _GetDataSource()
        {
            var ds = new SQLite_DataSource();
            ds.Initialize(this);
            return ds;
        }
    }
}