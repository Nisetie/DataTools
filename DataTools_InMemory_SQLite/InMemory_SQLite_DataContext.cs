using DataTools.Interfaces;
using System;

namespace DataTools.InMemory_SQLite
{
    public class InMemory_SQLite_DataContext : DataTools.Common.DataContext, IDisposable
    {
        private readonly string _connectionString;
        /// <summary>
        /// постоянное подключение нужно, чтобы БД не удалялись в промежутках между вызовами
        /// </summary>
        private InMemory_SQLite_DataSource _sConnection;

        public InMemory_SQLite_DataContext()
        {
            _connectionString = "FullUri=file:memdb?mode=memory&cache=shared";
            _sConnection = _GetDataSource() as InMemory_SQLite_DataSource;
        }

        protected override IDataSource _GetDataSource()
        {
            var conn = new InMemory_SQLite_DataSource(_connectionString);
            return conn;
        }

        public void Dispose()
        {
            _sConnection.Connection.Close();
        }
    }
}
