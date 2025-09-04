using DataTools.Common;
using DataTools.Deploy;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.PostgreSQL;
using NUnit.Framework.Internal;

namespace DataTools_Tests
{
    [TestFixture("postgresql", "Username=postgres;Password=1qaz@WSX;Host=localhost;")]
    public class PostgreSQLTests : CommonTests<PostgreSQL_DataContext>
    {

        public override GeneratorBase GetGenerator()
        {
            return new PostgreSQL_Generator(DataContext.ConnectionString);
        }

        public override IDBMS_QueryParser GetQueryParser()
        {
            return new PostgreSQL_QueryParser();
        }

        public PostgreSQLTests(string alias, string connectionString) : base(alias)
        {
            this.alias = alias;
            DataContext.ConnectionString = connectionString;
            //DataContext.AddCustomTypeConverter<DateTime>(
            //    ToLocalTime
            //    );

        }

        private object ToLocalTime(object dt)
        {
            return ((DateTime)dt).ToLocalTime();
        }

        [SetUp]
        public override void Setup()
        {
            var dc = DataManager.GetContext(alias) as PostgreSQL_DataContext;
            var ds = dc.GetDataSource() as PostgreSQL_DataSource;

            DataContext.Execute(new SqlCustom(@"
do $$
begin
if (to_regnamespace('dbo') is null) then
    create schema dbo;
end if;
end;
$$;
"));
            base.Setup();

            //            var sql = @"

            //insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 1,1,'TestModel1','a',false,1,1.1,1.2,1.3,'2024-01-01','23:59:59',gen_random_uuid(),'\x01020304';
            //insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 2,2,'TestModel2','b',true,1,1.1,1.2,1.3,'2024-03-01','01:01:01',gen_random_uuid(),'\xFFFFFFFF';
            //insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select NULL,NULL,'TestModel3','b',true,1,1.1,1.3,1.4,'2024-04-01','01:01:01',gen_random_uuid(),'\x0000FF00';

            //insert into dbo.TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra1',1,1.1,1.2,'2024-01-01',1;
            //insert into dbo.TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra2',1,1.1,1.2,'2023-01-01',2;

            //insert into dbo.TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild1',1,1.1,1.2,'2024-01-01',1,1;
            //insert into dbo.TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild2',1,1.15,1.25,'2023-01-01',3,2;

            //insert into dbo.TestModelGuidParent(id,name) select gen_random_uuid(), 'parent1';
            //insert into dbo.TestModelGuidParent(id,name) select gen_random_uuid(), 'parent2';

            //insert into dbo.TestModelGuidChild(id,name,parent_id) select gen_random_uuid(), 'child1', (select id from dbo.TestModelGuidParent where name = 'parent1');
            //insert into dbo.TestModelGuidChild(id,name,parent_id) select gen_random_uuid(), 'child2', (select id from dbo.TestModelGuidParent where name = 'parent2');

            //insert into dbo.TestModelNoUnique (id,name) select 1,'a';
            //insert into dbo.TestModelNoUnique (id,name) select 2,'b';
            //insert into dbo.TestModelNoUnique (id,name) select 1,'c';
            //insert into dbo.TestModelNoUnique (id,name) select 3,'b';
            //insert into dbo.TestModelNoUnique (id,name) select null,'d';


            //";
            //            sql += @"
            //insert into dbo.TestModelParentCompositePrimaryKey(i,j,k) select 1,1,'abc';
            //insert into dbo.TestModelParentCompositePrimaryKey(i,j,k) select 1,2,'def';
            //insert into dbo.TestModelParentCompositePrimaryKey(i,j,k) select 2,2, 'ghi';
            //insert into dbo.TestModelParentCompositePrimaryKey(i,j,k) select 3,3, 'jkl';
            //";

            //            sql += @"
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 1,1,1,1,'abc';
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 2,2,1,1,'def';
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 1,2,1,2,'ghi';
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 2,3,2,2,'jkl';
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 3,3,3,3,'mno';
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 4,1,null,null,'pqr';
            //insert into dbo.TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 5,5,1,2,'stu';
            //";

            //            sql += @"
            //insert into dbo.TestModelCompositePrimaryKey(i,j,k) select 1,1,'abc';
            //insert into dbo.TestModelCompositePrimaryKey(i,j,k) select 2,2,'def';
            //insert into dbo.TestModelCompositePrimaryKey(i,j,k) select 1,2,'ghi';
            //insert into dbo.TestModelCompositePrimaryKey(i,j,k) select 2,3,'jkl';
            //insert into dbo.TestModelCompositePrimaryKey(i,j,k) select 3,3,'mno';
            //insert into dbo.TestModelCompositePrimaryKey(i,j,k) select 4,1,'pqr';
            //";

            //            sql += @"
            //insert into dbo.TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 1,1,'abc';
            //insert into dbo.TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 2,2,'def';
            //insert into dbo.TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 1,2,'ghi';
            //insert into dbo.TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 2,3,'jkl';
            //insert into dbo.TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 3,3,'mno';
            //insert into dbo.TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 4,1,'pqr';
            //";

            //            ds.Execute(sql);


            ds.Execute(@"
create or replace procedure dbo.testProc()
language plpgsql
as $$
declare i int;
begin
    i = 5;
end;
$$;
");

            ds.Execute(@"
create or replace procedure dbo.testProcReturn(i int, out recs json)
language plpgsql
as $$
declare
begin
	select to_json(t) from dbo.testmodel t where id = i into recs;
end
$$;

");


            ds.Execute(@"
create or replace function dbo.testFunc()
returns setof dbo.TestModel
language plpgsql
as $$
begin
    return query (select * from dbo.TestModel);
end
$$
");


            ds.Execute(@"
create or replace function dbo.testFuncParam(i int)
returns setof dbo.TestModel
language plpgsql
as $$
begin
    return query (select * from dbo.TestModel where id = i);
end
$$
");
        }

        [TearDown]
        public override void Teardown()
        {
            base.Teardown();

        }

        public override void TeardownScripts()
        {
            base.TeardownScripts();

            DataContext.Execute(new SqlCustom("drop function if exists dbo.testFunc;"));
            DataContext.Execute(new SqlCustom("drop function if exists dbo.testFuncParam;"));
            DataContext.Execute(new SqlCustom("drop procedure if exists dbo.testProc;"));
            DataContext.Execute(new SqlCustom("drop procedure if exists dbo.testProcReturn;"));
        }
    }
}

