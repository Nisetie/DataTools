using DataTools.Attributes;
using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataTools.Deploy
{
    public class ModelDefinition
    {
        public string Catalog { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }
        public string ModelCode { get; set; }
        public IModelMetadata ModelMetadata { get; set; }
    }

    public abstract class GeneratorBase
    {
        private List<ModelDefinition> models;

        protected abstract string GetMetadataQuery();

        public delegate Type SqlTypeParser(string sqlType);
        public delegate DBType SqlDBTypeParser(string sqlType);

        protected abstract SqlTypeParser GetSqlTypeParser();
        protected abstract SqlDBTypeParser GetDBTypeParser();

        protected abstract IDataContext GetDataContext();

        private Dictionary<string, TableCachedInfo> tableCaches;

        public IEnumerable<ModelDefinition> GetModelDefinitions(string objectIncludeNameFilter = "", string schemaIncludeNameFilter = "", string objectExcludeNameFilter = "", string schemaExcludeNameFilter = "")
        {
            string query = GetMetadataQuery();

            Dictionary<string, ModelMetadata> modelMetas = new Dictionary<string, ModelMetadata>();
            ModelMetadata modelMetadata = null;

            StringBuilder modelCode = new StringBuilder();

            string tableCatalog = "";
            string tableSchema = "";
            string tableName = "";
            string colName = "";
            string foreignTableSchema = "";
            string foreignTableName = "";
            string foreignModelName = "";

            var context = GetDataContext();

            var typeParser = GetSqlTypeParser();
            var dbtypeParser = GetDBTypeParser();

            // промежуточная обработка метаданных
            int ordinalPosition = 0; // собственный перечислитель на случай составных внешних ключей
            tableCaches = new Dictionary<string, TableCachedInfo>();

            foreach (var row in context.Select<Metadata>(new SqlCustom(query)))
            {
                if (row.TABLE_SCHEMA != null)
                {
                    if (!string.IsNullOrEmpty(schemaIncludeNameFilter))
                        if (!row.TABLE_SCHEMA.Contains(schemaIncludeNameFilter))
                            continue;
                    if (!string.IsNullOrEmpty(schemaExcludeNameFilter))
                        if (row.TABLE_SCHEMA.Contains(schemaExcludeNameFilter))
                            continue;
                }

                if (!string.IsNullOrEmpty(objectIncludeNameFilter))
                    if (!row.TABLE_NAME.Contains(objectIncludeNameFilter))
                        continue;
                if (!string.IsNullOrEmpty(objectExcludeNameFilter))
                    if (row.TABLE_NAME.Contains(objectExcludeNameFilter))
                        continue;

                if (!tableCaches.TryGetValue($"{row.TABLE_SCHEMA}.{row.TABLE_NAME}", out var tableCache))
                {
                    tableCaches[$"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"] = tableCache = new TableCachedInfo();
                    tableCache.Catalog = row.TABLE_CATALOG;
                    tableCache.Schema = row.TABLE_SCHEMA;
                    tableCache.Name = row.TABLE_NAME;
                    tableCache.IsView = row.IsView;
                    ordinalPosition = 0;
                }

                // группировка внешних ключей (если они составные)
                if (row.isForeign)
                {
                    if (!tableCache.Columns.TryGetValue(row.foreignKeyConstraintName, out var foreignColumnCache))
                    {
                        tableCache.Columns[row.foreignKeyConstraintName] = foreignColumnCache = new ForeignColumnCachedInfo();
                        foreignColumnCache.IsForeignKey = true;
                        foreignColumnCache.OrdinalPosition = ordinalPosition++; //row.ORDINAL_POSITION;
                        (foreignColumnCache as ForeignColumnCachedInfo).ConstraintName = row.foreignKeyConstraintName;
                        (foreignColumnCache as ForeignColumnCachedInfo).SchemaName = row.TABLE_SCHEMA;
                        (foreignColumnCache as ForeignColumnCachedInfo).TableName = row.TABLE_NAME;
                        (foreignColumnCache as ForeignColumnCachedInfo).ForeignSchemaName = row.foreignTableSchema;
                        (foreignColumnCache as ForeignColumnCachedInfo).ForeignTableName = row.foreignTableName;
                    }
                    (foreignColumnCache as ForeignColumnCachedInfo).Metadatas.Add(row);
                }
                else
                {
                    var columnCachedInfo = tableCache.Columns[row.COLUMN_NAME] = new ColumnCachedInfo();
                    columnCachedInfo.IsForeignKey = false;
                    columnCachedInfo.OrdinalPosition = ordinalPosition++; //row.ORDINAL_POSITION;
                    columnCachedInfo.Metadata = row;
                }
            }


            // из-за потенциальной рекурсивности связей между таблицами
            // необходимо воспрозводить метамодели сущностей поэтапно

            //сначала описать только экземпляры сущностей без деталей
            foreach (var tableCache in tableCaches.Values)
            {
                tableCatalog = tableCache.Catalog;
                tableSchema = tableCache.Schema;
                tableName = tableCache.Name;

                modelMetadata = new ModelMetadata();
                modelMetadata.ObjectName = tableName;
                modelMetadata.SchemaName = tableSchema;
                modelMetadata.IsView = tableCache.IsView;

                if (modelMetas.ContainsKey(modelMetadata.FullObjectName)) continue;

                modelMetadata.ModelTypeName = tableName;
                modelMetadata.DisplayModelName = tableName;

                modelMetas[modelMetadata.FullObjectName] = modelMetadata;
            }

            // потом добавить информацию о колонках без внешних связей
            foreach (var tableCache in tableCaches.Values)
            {
                tableCatalog = tableCache.Catalog;
                tableSchema = tableCache.Schema;
                tableName = tableCache.Name;

                modelMetadata = new ModelMetadata();
                modelMetadata.ObjectName = tableName;
                modelMetadata.SchemaName = tableSchema;

                modelMetadata = modelMetas[modelMetadata.FullObjectName];

                foreach (var column in from c in tableCache.Columns orderby c.Value.OrdinalPosition select c.Value)
                {
                    var modelField = new ModelFieldMetadata();

                    // пока что пропускаем внешние ключи
                    if (column is ForeignColumnCachedInfo) continue;

                    var columnMetadata = column.Metadata;
                    colName = columnMetadata.COLUMN_NAME;

                    Type netType = typeParser(columnMetadata.DATA_TYPE);
                    if (netType == null) throw new NotSupportedException();
                    string fieldTypeName = netType.FullName;
                    bool isValueType = netType.IsValueType;

                    modelField.FieldName = modelField.ColumnName = colName;
                    modelField.ColumnDisplayName = colName;
                    modelField.FieldTypeName = netType.AssemblyQualifiedName;

                    modelField.ColumnType = dbtypeParser(columnMetadata.DATA_TYPE);
                    if (modelField.ColumnType == null) throw new NotSupportedException();
                    modelField.FieldOrder = columnMetadata.ORDINAL_POSITION;
                    modelField.TextLength = columnMetadata.DATA_LENGTH;
                    modelField.NumericPrecision = columnMetadata.NUMERIC_PRECISION;
                    modelField.NumericScale = columnMetadata.NUMERIC_SCALE;

                    modelField.IgnoreChanges = columnMetadata.Generated || columnMetadata.isIdentity;
                    modelField.IsAutoincrement = columnMetadata.isIdentity;
                    modelField.IsPrimaryKey = columnMetadata.isPrimaryKey;
                    modelField.IsUnique = columnMetadata.isUnique;
                    modelField.UniqueConstraintName = columnMetadata.UniqueConstraintName;

                    modelMetadata.AddField(modelField);
                }
            }

            // потом добавить информацию о колонках c внешними связями
            foreach (var tableCache in tableCaches.Values)
            {
                tableCatalog = tableCache.Catalog;
                tableSchema = tableCache.Schema;
                tableName = tableCache.Name;

                if (tableSchema != null && !tableSchema.Contains(schemaIncludeNameFilter)) continue;
                if (!tableName.Contains(objectIncludeNameFilter)) continue;

                modelMetadata = new ModelMetadata();
                modelMetadata.ObjectName = tableName;
                modelMetadata.SchemaName = tableSchema;

                modelMetadata = modelMetas[modelMetadata.FullObjectName];

                foreach (var column in from c in tableCache.Columns orderby c.Value.OrdinalPosition select c.Value)
                {
                    var modelField = new ModelFieldMetadata();

                    // пропускаем не внешние ключи
                    if (!(column is ForeignColumnCachedInfo foreignColumn)) continue;

                    modelField.IsForeignKey = true;

                    var columnMetadata = column.Metadata;
                    colName = foreignColumn.ConstraintName;
                    colName = colName.Replace(" ", "_");

                    modelField.FieldName = modelField.ColumnName = colName;
                    modelField.ColumnDisplayName = colName;
                    modelField.FieldOrder = foreignColumn.Metadatas[0].ORDINAL_POSITION;

                    if (foreignColumn.Metadatas.Any(m => m.isPrimaryKey))
                        modelField.IsPrimaryKey = true;
                    if (foreignColumn.Metadatas.Any(m => m.isUnique))
                        modelField.IsUnique = true;


                    // ищем внешнюю модель в коллекции метаданных с помощью заглушки
                    var foreignModelMetadata = new ModelMetadata();
                    foreignModelMetadata.SchemaName = foreignTableSchema = foreignColumn.Metadatas[0].foreignTableSchema;
                    foreignModelMetadata.ObjectName = foreignModelMetadata.ModelTypeName = foreignTableName = foreignColumn.Metadatas[0].foreignTableName;

                    if (modelMetas.ContainsKey(foreignModelMetadata.FullObjectName))
                        // и подменяем заглушку заполненной метамоделью
                        modelField.ForeignModel = modelMetas[foreignModelMetadata.FullObjectName];
                    else
                        modelField.ForeignModel = modelMetas[foreignModelMetadata.FullObjectName] = foreignModelMetadata;

                    modelField.ForeignColumnNames = foreignColumn.Metadatas.Select(colm => colm.foreignColumnName).Distinct().ToArray();
                    modelField.ColumnNames = foreignColumn.Metadatas.Select(colm => colm.COLUMN_NAME).Distinct().ToArray();

                    foreignModelName = $"{tableCatalog}.{foreignTableSchema}.{foreignTableName}";
                    modelField.FieldTypeName = foreignModelName;

                    modelMetadata.AddField(modelField);
                }
            }

            var modelDefinitions = new List<ModelDefinition>();

            //потом всё остальное...
            foreach (var tableCache in tableCaches.Values)
            {
                modelCode.Clear();

                tableCatalog = tableCache.Catalog;
                tableSchema = tableCache.Schema;
                tableName = tableCache.Name;

                if (tableSchema != null && !tableSchema.Contains(schemaIncludeNameFilter)) continue;
                if (!tableName.Contains(objectIncludeNameFilter)) continue;

                modelMetadata = new ModelMetadata();
                modelMetadata.ObjectName = tableName;
                modelMetadata.SchemaName = tableSchema;

                modelMetadata = modelMetas[modelMetadata.FullObjectName];

                if (!string.IsNullOrEmpty(tableSchema))
                    modelCode
                    .AppendLine($"using {nameof(DataTools)}.{nameof(DataTools.Attributes)};")
                    .AppendLine()
                    .AppendLine($"namespace {tableCatalog}.{tableSchema} {{")
                    .AppendLine($"\t[{nameof(ObjectNameAttribute)}(\"{tableName}\",\"{tableSchema}\")]")
                    .AppendLine($"{(modelMetadata.IsView ? $"\t[{nameof(NoUniqueAttribute)}]" : string.Empty)}")
                    .AppendLine($"\tpublic class {tableName.Replace(" ", "_")} {{")
                    .AppendLine();
                else
                    modelCode
                    .AppendLine($"using {nameof(DataTools)}.{nameof(DataTools.Attributes)};")
                    .AppendLine()
                    .AppendLine($"namespace {tableCatalog} {{")
                    .AppendLine($"\t[{nameof(ObjectNameAttribute)}(\"{tableName}\")]")
                    .AppendLine($"{(modelMetadata.IsView ? $"\t[{nameof(NoUniqueAttribute)}]" : string.Empty)}")
                    .AppendLine($"\tpublic class {tableName.Replace(" ", "_")} {{")
                    .AppendLine();

                foreach (var column in from c in tableCache.Columns orderby c.Value.OrdinalPosition select c.Value)
                {
                    if ((column is ForeignColumnCachedInfo foreignColumn))
                    {
                        var modelField = modelMetadata.Fields.Where(f => f.FieldName == foreignColumn.ConstraintName).Single();
                        colName = modelField.ColumnName;
                        var foreignFieldNames = new StringBuilder();
                        var columnNames = new StringBuilder(); ;
                        int i = 0;
                        foreach (var columnName in modelField.ColumnNames)
                        {
                            var columnMetadata = foreignColumn.Metadatas[i];
                            var foreignField = modelField.ForeignModel.GetColumn(modelField.ForeignColumnNames[i]);
                            foreignFieldNames.Append($"\"{foreignField.ColumnName}\",");
                            columnNames.Append($"\"{modelField.ColumnNames[i]}\",");
                            i++;
                        }
                        foreignFieldNames.Length -= 1;
                        columnNames.Length -= 1;


                        modelCode.AppendLine($"\t\t[{nameof(OrdinalPositionAttribute)}({modelField.FieldOrder})]");

                        modelCode.AppendLine($"\t\t[{nameof(ReferenceAttribute)}(new string[] {{ {foreignFieldNames} }}, new string[]{{ {columnNames} }} )]");
                        if (modelField.IsPrimaryKey)
                            modelCode.AppendLine($"\t\t[{nameof(PrimaryKeyAttribute)}]");

                        if (modelField.IsUnique)
                            modelCode.AppendLine($"\t\t[{nameof(UniqueAttribute)}]");

                        foreignTableName = foreignColumn.ForeignTableName;
                        foreignModelName = $"{tableCatalog}.{foreignTableSchema}.{foreignTableName}";

                        modelCode.AppendLine($"\t\tpublic {foreignModelName} {foreignTableName} {{get; set;}}");
                    }
                    else
                    {
                        var modelField = modelMetadata.Fields.Where(f => f.ColumnName == column.Metadata.COLUMN_NAME).Single();
                        colName = modelField.ColumnName;

                        var columnMetadata = column.Metadata;
                        Type netType = typeParser(columnMetadata.DATA_TYPE);
                        string fieldTypeName = netType.FullName;

                        if (modelField.TextLength != null)
                            modelCode.AppendLine($"\t\t[{nameof(TextLengthAttribute)}({modelField.TextLength})]");

                        if (modelField.NumericPrecision != null)
                        {
                            if (netType is decimal)
                            {
                                modelCode.AppendLine($"\t\t[{nameof(NumericAttribute)}({columnMetadata.NUMERIC_PRECISION},{columnMetadata.NUMERIC_SCALE})]");
                            }
                        }

                        modelCode.AppendLine($"\t\t[{nameof(OrdinalPositionAttribute)}({modelField.FieldOrder})]");

                        if (columnMetadata.Generated || columnMetadata.isIdentity)
                            modelCode.AppendLine($"\t\t[{nameof(IgnoreChangesAttribute)}]");

                        if (columnMetadata.isIdentity)
                            modelCode.AppendLine($"\t\t[{nameof(AutoincrementAttribute)}]");

                        if (columnMetadata.isPrimaryKey)
                            modelCode.AppendLine($"\t\t[{nameof(PrimaryKeyAttribute)}]");

                        if (columnMetadata.isUnique)
                        {
                            modelCode.AppendLine($"\t\t[{nameof(UniqueAttribute)}(\"{modelField.UniqueConstraintName}\")]");
                        }

                        modelCode.AppendLine($"\t\t[{nameof(ColumnNameAttribute)}(\"{colName}\")]");
                        modelCode.AppendLine($"\t\t[{nameof(ColumnTypeAttribute)}(\"{modelField.ColumnType.Name}\")]");
                        modelCode.AppendLine($"\t\tpublic {fieldTypeName}{(columnMetadata.isNullable && netType.IsValueType ? "?" : "")} {colName.Replace(' ', '_')} {{get; set;}}");
                    }
                    modelCode.AppendLine();
                }

                modelCode.AppendLine("\t}").AppendLine("}");
                yield return new ModelDefinition() { Catalog = tableCatalog, Schema = tableSchema, Name = tableName, ModelCode = modelCode.ToString(), ModelMetadata = modelMetadata };
            }
        }

        internal class TableCachedInfo
        {
            public string Catalog { get; set; }
            public string Schema { get; set; }
            public string Name { get; set; }

            public bool IsView { get; set; }
            public Dictionary<string, ColumnCachedInfo> Columns { get; set; } = new Dictionary<string, ColumnCachedInfo>();
        }

        internal class ColumnCachedInfo
        {
            public string SchemaName { get; set; }
            public string TableName { get; set; }
            public Metadata Metadata { get; set; }
            public bool IsForeignKey { get; set; }
            public int OrdinalPosition { get; set; }
        }

        internal class ForeignColumnCachedInfo : ColumnCachedInfo
        {
            public string ConstraintName { get; set; }
            public List<string> ColumnNames { get; set; } = new List<string>();
            public string ForeignSchemaName { get; set; }
            public string ForeignTableName { get; set; }
            public List<string> ForeignColumnNames { get; set; } = new List<string>();
            public List<Metadata> Metadatas { get; set; } = new List<Metadata>();
        }

        [NoUnique]
        internal class Metadata
        {
            public string TABLE_CATALOG { get; set; }
            public string TABLE_SCHEMA { get; set; }
            public string TABLE_NAME { get; set; }
            public string TABLE_TYPE { get; set; }
            public bool IsView { get; set; }
            public string COLUMN_NAME { get; set; }
            public string DATA_TYPE { get; set; }
            public int? DATA_LENGTH { get; set; }
            public int? NUMERIC_PRECISION { get; set; }
            public int? NUMERIC_SCALE { get; set; }
            public int ORDINAL_POSITION { get; set; }
            public bool isNullable { get; set; }
            public bool Generated { get; set; }
            public bool isPrimaryKey { get; set; }
            public bool isUnique { get; set; }
            public string UniqueConstraintName { get; set; }
            public bool isForeign { get; set; }
            public bool isIdentity { get; set; }
            public string foreignTableSchema { get; set; }
            public string foreignTableName { get; set; }
            public string foreignColumnName { get; set; }
            public string foreignKeyConstraintName { get; set; }
        }
    }
}