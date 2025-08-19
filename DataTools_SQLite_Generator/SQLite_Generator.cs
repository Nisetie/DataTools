using DataTools.Interfaces;
using DataTools.SQLite;

namespace DataTools.Deploy
{
    public class SQLite_Generator : GeneratorBase
    {
        private SQLite_DataContext _context;

        public SQLite_Generator(string connectionString)
        {
            _context = new SQLite_DataContext(connectionString);
        }

        protected override IDataContext GetDataContext()
        {
            return _context;
        }

        protected override string GetMetadataQuery()
        {
            return @"
with tablesAndColums as (
    select 
	    table_catalog
	    ,TABLE_SCHEMA
	    ,TABLE_NAME
	    ,TABLE_TYPE
	    ,COLUMN_TYPE
	    ,COLUMN_NAME
	    ,ORDINAL_POSITION
	    ,IS_NULLABLE
	    ,Generated
	    , iif(instr(DATA_LENGTH , ',')>0,null, DATA_LENGTH ) as DATA_LENGTH
	    , iif(instr(COLUMN_TYPE,'('), substr(COLUMN_TYPE, 1,instr(COLUMN_TYPE,'(')-1), COLUMN_TYPE) as DATA_TYPE
	    , iif(instr(DATA_LENGTH , ',') >0, substr(DATA_LENGTH, 1, instr(DATA_LENGTH,',')-1) ,null) as NUMERIC_PRECISION
	    , iif(instr(DATA_LENGTH , ',') >0, substr(DATA_LENGTH, instr(DATA_LENGTH,',')+1) ,null) as NUMERIC_SCALE
    from (
	    select
		    'main' as table_catalog
		    ,null as TABLE_SCHEMA
		    ,t.name as TABLE_NAME
		    ,t.type as TABLE_TYPE
		    ,c.type as COLUMN_TYPE
		    ,c.name as COLUMN_NAME
		    ,c.cid as ORDINAL_POSITION
		    ,iif(c.[notnull] = 1, 0, 1) as IS_NULLABLE
		    ,iif(c.dflt_value is not null, 1, 0) as Generated
		    , iif(instr(c.type,'(') > 0,substr(c.type, instr(c.type,'(')+1,instr(c.type,')') - instr(c.type,'(') -1),null) as DATA_LENGTH
	    from sqlite_master t
	    join pragma_table_info(t.name) c
	    where t.type in ('table','view')
    ) 
)
,primaryKeys as (
    select
        'main' as CONSTRAINT_CATALOG
        ,concat(t.name,'_',c.name,'_pk') as CONSTRAINT_NAME
        ,t.name as TABLE_NAME
        ,null as TABLE_SCHEMA
        ,c.name as COLUMN_NAME 
		,count() over (partition by t.name) as primaryKeysCount
    from sqlite_master t
    join pragma_table_xinfo(t.name) c
    where c.pk > 0
)
, foreignKeys as (
   	select
        'main' as CONSTRAINT_CATALOG
        ,concat(t.name,'_',fk.id,'_fk') as CONSTRAINT_NAME
        ,t.name as TABLE_NAME
        ,null as TABLE_SCHEMA
        ,c.name as COLUMN_NAME 
        ,null as foreignTableSchema
        ,fk.[table] as foreignTableName
        ,fk.[to] as foreignColumnName
    from sqlite_master t
    join pragma_table_xinfo(t.name) c
	join pragma_foreign_key_list(t.name) fk	
	where fk.[from] = c.name
)
,[unique] as (
	SELECT DISTINCT
		'main' as CONSTRAINT_CATALOG
		,il.name as CONSTRAINT_NAME 
		,t.name as TABLE_NAME
		,null as TABlE_SCHEMA
		,ii.name as COLUMN_NAME
  FROM sqlite_master as t
  join pragma_index_list(t.name) as il
  join pragma_index_info(il.name) as ii
  where il.[unique] = 1  
)
SELECT 
tablesAndColums.TABLE_CATALOG
,tablesAndColums.TABLE_SCHEMA
,tablesAndColums.TABLE_NAME
,tablesAndColums.TABLE_TYPE
,iif(tablesAndColums.TABLE_TYPE='view',1,0) as IsView
,tablesAndColums.COLUMN_NAME
,tablesAndColums.DATA_TYPE
,tablesAndColums.DATA_LENGTH
,tablesAndColums.NUMERIC_PRECISION
,tablesAndColums.NUMERIC_SCALE
,tablesAndColums.ORDINAL_POSITION
,tablesAndColums.IS_NULLABLE as isNullable
,tablesAndColums.Generated as isGenerated
,iif(primaryKeys.CONSTRAINT_NAME is not null, 1, 0) as isPrimaryKey
,iif([unique].CONSTRAINT_CATALOG is not null and coalesce(primaryKeys.primaryKeysCount,0) < 2, 1, 0) as isUnique
,[unique].CONSTRAINT_NAME as UniqueConstraintName
,iif(foreignKeys.CONSTRAINT_NAME is not null, 1, 0) as isForeign
,iif((tablesAndColums.data_type = 'INT' or tablesAndColums.data_type = 'INTEGER') and primaryKeys.CONSTRAINT_NAME is not null and primaryKeys.primaryKeysCount = 1, 1,0) as isIdentity
,foreignKeys.foreignTableSchema
,foreignKeys.foreignTableName
,foreignKeys.foreignColumnName
,foreignKeys.CONSTRAINT_NAME as foreignKeyConstraintName
, count(foreignKeys.foreignTableName) over (partition by tablesAndColums.TABLE_CATALOG, tablesAndColums.TABLE_SCHEMA, tablesAndColums.TABLE_NAME) as referenceCount
FROM
tablesAndColums 
left join primaryKeys on tablesAndColums.TABLE_NAME = primaryKeys.TABLE_NAME and tablesAndColums.COLUMN_NAME = primaryKeys.COLUMN_NAME
left join [unique] on tablesAndColums.TABLE_NAME = [unique].TABLE_NAME and tablesAndColums.COLUMN_NAME = [unique].COLUMN_NAME
left join foreignKeys on tablesAndColums.TABLE_NAME = foreignKeys.TABLE_NAME and tablesAndColums.COLUMN_NAME = foreignKeys.COLUMN_NAME
order by referenceCount, tablesAndColums.TABLE_SCHEMA, tablesAndColums.TABLE_NAME, tablesAndColums.ORDINAL_POSITION
";
        }

        protected override SqlTypeParser GetSqlTypeParser()
        {
            return SQLite_TypesMap.GetNetType;
        }

        protected override SqlDBTypeParser GetDBTypeParser()
        {
            return SQLite_TypesMap.GetDBType;
        }
    }
}
