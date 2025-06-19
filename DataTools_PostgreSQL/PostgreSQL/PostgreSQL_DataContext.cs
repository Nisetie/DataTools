using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Linq;

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

        public override void CreateTable<ModelT>()
        {
            var meta = ModelMetadata<ModelT>.Instance;

            var name = meta.FullObjectName;

            var fieldsDefinition = string.Join($",{Environment.NewLine}",
             from field
             in meta.Fields
             let isReference = field.IsForeignKey
             let referenceField = isReference ? field.ForeignModel.GetColumn(field.ForeignColumnName) : null
             let dataType = isReference ? (referenceField.ColumnType ?? PostgreSQL_TypesMap.GetSqlType(referenceField.FieldType)) : field.ColumnType ?? PostgreSQL_TypesMap.GetSqlType(field.FieldType)
             let isUniqId = PostgreSQL_TypesMap.IsNumber(dataType) && field.IsAutoincrement
             let uniqId = isUniqId ? "primary key generated always as identity" : ""
             let isUniq = field.IsUnique
             let uniq = isUniq && !isUniqId ? "UNIQUE" : ""
             let reference = isReference ? $"references {field.ForeignModel.FullObjectName}({field.ForeignModel.GetColumn(field.ForeignColumnName).ColumnName})" : ""
             select $"{field.ColumnName} {dataType} {uniqId} {uniq} {reference}"
         );


            this.Execute(new SqlCustom($"CREATE TABLE IF NOT EXISTS {name} ({fieldsDefinition});"));
        }
        public override void DropTable<ModelT>()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            var name = meta.FullObjectName;
            Execute(new SqlCustom($"drop table if exists {name};"));
        }
    }
}