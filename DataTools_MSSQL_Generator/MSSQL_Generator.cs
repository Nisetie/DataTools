using DataTools.Interfaces;
using DataTools.MSSQL;

namespace DataTools.Deploy
{
    public class MSSQL_Generator : GeneratorBase
    {
        public string ConnectionString { get; set; }

        protected override IDataContext GetDataContext()
        {
            return new MSSQL_DataContext(ConnectionString);
        }

        protected override string GetMetadataQuery()
        {
            return @"
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
		left_t.CONSTRAINT_CATALOG
		,left_t.CONSTRAINT_NAME
		,left_t.TABLE_NAME
		,left_t.TABLE_SCHEMA
		,left_t.COLUMN_NAME 
		,right_t.TABLE_SCHEMA as foreignTableSchema
		,right_t.TABLE_NAME as foreignTableName
		,right_t.COLUMN_NAME as foreignColumnName
	from (
	   	select      
			tc.CONSTRAINT_NAME
			,tc.CONSTRAINT_CATALOG			
			,tc.TABLE_SCHEMA
			,tc.TABLE_NAME			
			,left_kku.COLUMN_NAME
			, row_number() over (partition by tc.table_catalog,tc.table_schema,tc.table_name,tc.CONSTRAINT_NAME order by left_kku.ordinal_position) as rn
	    from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc on tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		join INFORMATION_SCHEMA.KEY_COLUMN_USAGE left_kku on rc.constraint_name = left_kku.constraint_name
		where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
	) left_t,
	(
		select      
			tc.CONSTRAINT_NAME
			,right_kku.CONSTRAINT_CATALOG			
			,right_kku.TABLE_SCHEMA
			,right_kku.TABLE_NAME			
			,right_kku.COLUMN_NAME
			, row_number() over (partition by tc.table_catalog,tc.table_schema,tc.table_name,tc.CONSTRAINT_NAME order by right_kku.ordinal_position) as rn
	    from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc on tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		join INFORMATION_SCHEMA.KEY_COLUMN_USAGE right_kku on rc.unique_constraint_name = right_kku.constraint_name
		where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
	) right_t
	where left_t.rn = right_t.rn and left_t.CONSTRAINT_NAME = right_t.CONSTRAINT_NAME
	order by left_t.CONSTRAINT_CATALOG,left_t.TABLE_SCHEMA,left_t.TABLE_NAME,left_t.CONSTRAINT_NAME,left_t.rn	
)
, [tablesAndColums] as (
    select  top 100 percent
        t.table_catalog
        ,t.TABLE_SCHEMA
        ,t.TABLE_NAME
        ,t.TABLE_TYPE 
        ,c.COLUMN_NAME
        ,c.character_maximum_length as DATA_LENGTH
        ,c.NUMERIC_PRECISION as NUMERIC_PRECISION
        ,c.NUMERIC_SCALE as NUMERIC_SCALE
        ,c.DATA_TYPE as DATA_TYPE
        ,c.ORDINAL_POSITION
        ,iif(c.IS_NULLABLE = 'YES', 1, 0) as IS_NULLABLE
        ,IIF(c.column_default is not null, 1, 0) as [Generated]
    from INFORMATION_SCHEMA.TABLES t
    join INFORMATION_SCHEMA.COLUMNS c on t.TABLE_SCHEMA = c.TABLE_SCHEMA and t.TABLE_NAME = c.TABLE_NAME
	where t.TABLE_TYPE = 'BASE TABLE' -- ignore views
    order by t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
)
select
tablesAndColums.TABLE_CATALOG
,tablesAndColums.TABLE_SCHEMA
,tablesAndColums.TABLE_NAME
,tablesAndColums.TABLE_TYPE
,tablesAndColums.COLUMN_NAME
,tablesAndColums.DATA_TYPE
,tablesAndColums.DATA_LENGTH
,tablesAndColums.NUMERIC_PRECISION
,tablesAndColums.NUMERIC_SCALE
,tablesAndColums.ORDINAL_POSITION
,cast(tablesAndColums.IS_NULLABLE as bit) as isNullable
,cast(tablesAndColums.[Generated] as bit) as [isGenerated]
,cast(iif (primaryKeys.CONSTRAINT_NAME is not null,1, 0) as bit) as isPrimaryKey
,cast(iif ([unique].CONSTRAINT_NAME is not null,1, 0) as bit) as isUnique
,[unique].CONSTRAINT_NAME
,cast(iif (foreignKeys.CONSTRAINT_NAME is not null,1, 0) as bit) as isForeign
,cast(iif (COLUMNPROPERTY(OBJECT_ID(CONCAT(tablesAndColums.[TABLE_CATALOG],'.',tablesAndColums.[TABLE_SCHEMA],'.',tablesAndColums.[TABLE_NAME])) ,tablesAndColums.[COLUMN_NAME] ,'IsIdentity') = 1,1,0) as bit) as isIdentity
,foreignKeys.foreignTableSchema
,foreignKeys.foreignTableName
,foreignKeys.foreignColumnName
,foreignKeys.CONSTRAINT_NAME as foreignKeyConstraintName
, count(foreignKeys.foreignTableName) over (partition by tablesAndColums.TABLE_CATALOG, tablesAndColums.TABLE_SCHEMA, tablesAndColums.TABLE_NAME) as referenceCount
from tablesAndColums
left join primaryKeys on tablesAndColums.TABLE_SCHEMA = primaryKeys.TABLE_SCHEMA and tablesAndColums.TABLE_NAME = primaryKeys.TABLE_NAME and tablesAndColums.COLUMN_NAME = primaryKeys.COLUMN_NAME
left join [unique] on tablesAndColums.TABLE_SCHEMA = [unique].TABLE_SCHEMA and tablesAndColums.TABLE_NAME = [unique].TABLE_NAME and tablesAndColums.COLUMN_NAME = [unique].COLUMN_NAME
left join foreignKeys on tablesAndColums.TABLE_SCHEMA = foreignKeys.TABLE_SCHEMA and tablesAndColums.TABLE_NAME = foreignKeys.TABLE_NAME and tablesAndColums.COLUMN_NAME = foreignKeys.COLUMN_NAME
order by referenceCount, tablesAndColums.TABLE_SCHEMA, tablesAndColums.TABLE_NAME, tablesAndColums.ORDINAL_POSITION
";
        }

        protected override SqlTypeParser GetSqlTypeParser()
        {
            return MSSQL_TypesMap.GetNetTypeFromSqlType;
        }

        protected override SqlDBTypeParser GetDBTypeParser()
        {
            return MSSQL_TypesMap.GetDBTypeFromSqlType;
        }

        public MSSQL_Generator(string connectionString) { ConnectionString = connectionString; }
    }
}
