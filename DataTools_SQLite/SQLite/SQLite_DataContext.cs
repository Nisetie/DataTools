using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Data.Common;
using System.Linq;

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

            this.Execute(new SqlCustom($"CREATE TABLE IF NOT EXISTS {name} ({fieldsDefinition});"));
        }

        public override void DropTable<ModelT>()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            var name = meta.FullObjectName;
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');
            Execute(new SqlCustom($"drop table if exists {name};"));
        }
    }
}