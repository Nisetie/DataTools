using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Linq;

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
            var ds = new MSSQL_DataSource(ConnectionString);
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
                let dataType = isReference ? (referenceField.ColumnType ?? MSSQL_TypesMap.GetSqlType(referenceField.FieldType)) : field.ColumnType ?? MSSQL_TypesMap.GetSqlType(field.FieldType)
                let isUniqId = MSSQL_TypesMap.IsNumber(dataType) && field.IsAutoincrement
                let uniqId = isUniqId ? "primary key identity" : ""
                let isUniq = field.IsUnique
                let uniq = isUniq && !isUniqId ? "UNIQUE" : ""                
                let reference = isReference ? $"references {field.ForeignModel.FullObjectName}({field.ForeignModel.GetColumn(field.ForeignColumnName).ColumnName})" : ""
                select $"{field.ColumnName} {dataType} {uniqId} {uniq} {reference}"
            );

            var sql = new SqlCustom($"if object_id('{name}') is null CREATE TABLE {name} ({fieldsDefinition});");

            this.Execute(sql);
        }
        public override void DropTable<ModelT>()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            var name = meta.FullObjectName;
            Execute(new SqlCustom($"drop table if exists {name};"));
        }
    }
}