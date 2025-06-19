using DataTools.Attributes;
using DataTools.DML;
using DataTools.MSSQL;
using System.Text;
using System.Collections.Generic;
using DataTools.Interfaces;
using DataTools.Meta;

namespace mssqlgen
{
    public class MSSQL_Generator
    {
        public string NamespaceName { get; set; }
        public string ConnectionString { get; set; }

        public MSSQL_Generator(string namespaceName, string connectionString) { NamespaceName = namespaceName; ConnectionString = connectionString; }

        private readonly string _query = @"
with primaryKeys as (
    select top 100 percent
        tc.CONSTRAINT_CATALOG
        ,tc.CONSTRAINT_NAME
        ,ccu.TABLE_NAME
        ,ccu.TABLE_SCHEMA
        ,ccu.COLUMN_NAME 
    from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
    where tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    order by tc.CONSTRAINT_NAME, ccu.TABLE_CATALOG, ccu.TABLE_SCHEMA, ccu.TABLE_NAME, ccu.COLUMN_NAME
)
,[unique] as (
    select top 100 percent
        tc.CONSTRAINT_CATALOG
        ,tc.CONSTRAINT_NAME
        ,ccu.TABLE_NAME
        ,ccu.TABLE_SCHEMA
        ,ccu.COLUMN_NAME 
    from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
    where tc.CONSTRAINT_TYPE = 'UNIQUE'
    order by tc.CONSTRAINT_NAME, ccu.TABLE_CATALOG, ccu.TABLE_SCHEMA, ccu.TABLE_NAME, ccu.COLUMN_NAME
)
, [foreignKeys] as (
    select top 100 percent
        tc.CONSTRAINT_CATALOG
        ,tc.CONSTRAINT_NAME
        ,ccu.TABLE_NAME
        ,ccu.TABLE_SCHEMA
        ,ccu.COLUMN_NAME 
        ,ccu1.TABLE_SCHEMA as foreignTableSchema
        ,ccu1.TABLE_NAME as foreignTableName
        ,ccu1.COLUMN_NAME as foreignColumnName
    from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
    join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc on tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME --and ccu.COLUMN_NAME = rc.CONSTRAINT_NAME
    join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu1 on ccu1.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
    where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    order by tc.CONSTRAINT_NAME, ccu.TABLE_CATALOG, ccu.TABLE_SCHEMA, ccu.TABLE_NAME, ccu.COLUMN_NAME
)
, [tablesAndColums] as (
    select  top 100 percent
        t.table_catalog
        ,t.TABLE_SCHEMA
        ,t.TABLE_NAME
        ,t.TABLE_TYPE 
        ,c.COLUMN_NAME
        ,c.DATA_TYPE
        ,c.ORDINAL_POSITION
        ,iif(c.IS_NULLABLE = 'YES', 1, 0) as IS_NULLABLE
        ,IIF(c.column_default is not null, 1, 0) as [Generated]
    from INFORMATION_SCHEMA.TABLES t
    join INFORMATION_SCHEMA.COLUMNS c on t.TABLE_SCHEMA = c.TABLE_SCHEMA and t.TABLE_NAME = c.TABLE_NAME
    order by c.ORDINAL_POSITION
)
select
tablesAndColums.TABLE_CATALOG
,tablesAndColums.TABLE_SCHEMA
,tablesAndColums.TABLE_NAME
,tablesAndColums.TABLE_TYPE
,tablesAndColums.COLUMN_NAME
,tablesAndColums.DATA_TYPE
,tablesAndColums.ORDINAL_POSITION
,cast(tablesAndColums.IS_NULLABLE as bit)
--,cast(tablesAndColums.[Generated] as bit)
,cast(0 as bit) as [Generated]
,cast(iif (primaryKeys.CONSTRAINT_NAME is not null,1, 0) as bit) as isPrimaryKey
,cast(iif ([unique].CONSTRAINT_NAME is not null,1, 0) as bit) as isUnique
,cast(iif (foreignKeys.CONSTRAINT_NAME is not null,1, 0) as bit) as isForeign
,cast(iif (COLUMNPROPERTY(OBJECT_ID(CONCAT(tablesAndColums.[TABLE_CATALOG],'.',tablesAndColums.[TABLE_SCHEMA],'.',tablesAndColums.[TABLE_NAME])) ,tablesAndColums.[COLUMN_NAME] ,'IsIdentity') = 1,1,0) as bit) as isIdentity
,foreignKeys.foreignTableSchema
,foreignKeys.foreignTableName
,foreignKeys.foreignColumnName
from tablesAndColums
left join primaryKeys on tablesAndColums.TABLE_SCHEMA = primaryKeys.TABLE_SCHEMA and tablesAndColums.TABLE_NAME = primaryKeys.TABLE_NAME and tablesAndColums.COLUMN_NAME = primaryKeys.COLUMN_NAME
left join [unique] on tablesAndColums.TABLE_SCHEMA = [unique].TABLE_SCHEMA and tablesAndColums.TABLE_NAME = [unique].TABLE_NAME and tablesAndColums.COLUMN_NAME = [unique].COLUMN_NAME
left join foreignKeys on tablesAndColums.TABLE_SCHEMA = foreignKeys.TABLE_SCHEMA and tablesAndColums.TABLE_NAME = foreignKeys.TABLE_NAME and tablesAndColums.COLUMN_NAME = foreignKeys.COLUMN_NAME
order by tablesAndColums.TABLE_SCHEMA, tablesAndColums.TABLE_NAME, tablesAndColums.ORDINAL_POSITION
";
        public class ModelDefinition
        {
            public string Catalog { get; set; }
            public string Schema { get; set; }
            public string Name { get; set; }
            public string ModelCode { get; set; }
            public IModelMetadata ModelMetadata { get; set; }
        }

        public IEnumerable<ModelDefinition> GetModelDefinitions()
        {
            ModelMetadata mm = null;

            StringBuilder modelCode = new StringBuilder();

            string tableCatalog = "";
            string tableSchema = "";
            string tableName = "";
            string colName = "";
            string foreignTableSchema = "";
            string foreignTableName = "";
            string foreignColName = "";
            string foreignModelName = "";
            string fileName;

            var context = new MSSQL_DataContext(ConnectionString);

            foreach (var row in context.Select<Metadata>(new SqlCustom(_query)))
            {
                colName = row.COLUMN_NAME;

                if ($"{row.TABLE_CATALOG}.{row.TABLE_SCHEMA}.{row.TABLE_NAME}" != $"{tableCatalog}.{tableSchema}.{tableName}")
                {
                    if (modelCode.Length > 0)
                    {
                        modelCode.AppendLine("\t}");
                        modelCode.AppendLine("}");

                        yield return new ModelDefinition() { Catalog = tableCatalog, Schema = tableSchema, Name = tableName, ModelCode = modelCode.ToString(), ModelMetadata = mm};

                        modelCode.Clear();
                    }

                    mm = new ModelMetadata();

                    (tableCatalog, tableSchema, tableName) = (row.TABLE_CATALOG, row.TABLE_SCHEMA, row.TABLE_NAME);

                    mm.ObjectName = tableName;
                    mm.SchemaName = tableSchema;
                    mm.ModelName = tableName;
                    mm.DisplayModelName = tableName;

                    modelCode
                        .AppendLine($"using {nameof(DataTools)}.{nameof(DataTools.Attributes)};")
                        .AppendLine()
                        .AppendLine($"namespace {tableCatalog}{{")
                        .AppendLine($"\t[{nameof(ObjectNameAttribute)}(\"{tableName}\",\"{tableSchema}\")]")
                        .AppendLine($"\tpublic class {tableSchema}_{tableName} {{")
                        .AppendLine();
                }

                if (row.Generated || row.isIdentity) modelCode.AppendLine($"\t\t[{nameof(IgnoreChangesAttribute)}]");
                if (row.isPrimaryKey || row.isUnique) modelCode.AppendLine($"\t\t[{nameof(UniqueAttribute)}]");
                if (row.isForeign)
                {
                    foreignTableSchema = row.foreignTableSchema;
                    foreignTableName = row.foreignTableName;
                    foreignColName = row.foreignColumnName;
                    foreignModelName = $"{foreignTableSchema}_{foreignTableName}";

                    modelCode.AppendLine($"\t\t[{nameof(ReferenceAttribute)}(nameof({foreignModelName}.{foreignColName.Replace(' ', '_')})]");
                }

                var netType = MSSQL_TypesMap.GetNetType(row.DATA_TYPE);

                modelCode.AppendLine($"\t\t[{nameof(ColumnNameAttribute)}(\"{colName}\")]");
                modelCode.AppendLine($"\t\t{netType}{(row.IS_NULLABLE ? "?" : "")} {colName.Replace(' ', '_')} {{get; set;}}");

                modelCode.AppendLine();

                mm.AddField(new ModelFieldMetadata()
                {
                    ColumnName = colName,
                    ColumnDisplayName = colName,
                    FieldType = netType,
                    
                });


            }
        }

        [NoUnique]
        class Metadata
        {
            public string TABLE_CATALOG { get; set; }
            public string TABLE_SCHEMA { get; set; }
            public string TABLE_NAME { get; set; }
            public string TABLE_TYPE { get; set; }
            public string COLUMN_NAME { get; set; }
            public string DATA_TYPE { get; set; }
            public int ORDINAL_POSITION { get; set; }
            public bool IS_NULLABLE { get; set; }
            public bool Generated { get; set; }
            public bool isPrimaryKey { get; set; }
            public bool isUnique { get; set; }
            public bool isForeign { get; set; }
            public bool isIdentity { get; set; }
            public string foreignTableSchema { get; set; }
            public string foreignTableName { get; set; }
            public string foreignColumnName { get; set; }
        }
    }
}
