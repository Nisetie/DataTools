using DataTools.Interfaces;
using DataTools.Meta;
using DataTools.SQLite;
using System;
using System.Linq;

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
        
        public override void CreateTable<ModelT>()
        {
            var meta = ModelMetadata<ModelT>.Instance;

            var name = meta.FullObjectName;
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            var fieldsDefinition = string.Join(",",
                from field
                in meta.Fields
                let dataType = SQLite_TypesMap.GetSqlType(field.FieldType)
                let isUniqId = dataType == SQLite_TypesMap.INT && field.IsAutoincrement
                select $"{field.ColumnName} {(isUniqId ? "INTEGER PRIMARY KEY" : $"{dataType} {(field.IsUnique ? "UNIQUE" : "")}")}"
                );

            _sConnection.Execute($"CREATE TABLE IF NOT EXISTS {name} ({fieldsDefinition});");
        }

        public override void DropTable<ModelT>()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            var name = meta.FullObjectName;
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');
            _sConnection.Execute($"drop table if exists {name};");
        }

        public void Dispose()
        {
            _sConnection.Connection.Close();
        }
    }
}
