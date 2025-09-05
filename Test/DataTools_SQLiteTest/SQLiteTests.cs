using DataTools.Common;
using DataTools.Deploy;
using DataTools.Interfaces;
using DataTools.SQLite;
using NUnit.Framework.Internal;
using System.Data.SQLite;

namespace DataTools_Tests
{
    [TestFixture("sqlite", "Data Source=dbo;journal mode=WAL;synchronous=off;pooling=true")]
    public class SQLiteTests : CommonTests<SQLite_DataContext>
    {
        public override GeneratorBase GetGenerator()
        {
            return new SQLite_Generator(DataContext.ConnectionString);
        }

        public override IDBMS_QueryParser GetQueryParser()
        {
            return new SQLite_QueryParser();
        }

        private SQLiteConnection _conn;

        public SQLiteTests(string alias, string connectionString) : base(alias)
        {
            this.alias = alias;
            DataContext.ConnectionString = connectionString;
            _conn = new SQLiteConnection(connectionString);
            _conn.Open();

        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _conn.Close();
        }

        public override void Setup()
        {
            var dc = DataManager.GetContext(alias) as SQLite_DataContext;
            //dc.AddCustomMapper(delegate (IDataContext dataContext, object[] values)
            //{
            //    var model = new TestModel();
            //    model.Id = (int)values[0].Cast<long>();
            //    model.Value = values[6].Cast<int>();
            //    model.FValue = (float)values[7].Cast<double>();
            //    model.GValue = values[8].Cast<double>();
            //    model.Money = (decimal)values[9].Cast<double>();
            //    model.CharCode = values[4].Cast<string>();
            //    model.Checked = values[5].Cast<int>() > 0;
            //    if (TimeSpan.TryParse(values[11].Cast<string>(), out var result))
            //        model.Duration = result;
            //    if (Guid.TryParse(values[12].Cast<string>(), out var result2))
            //        model.Guid = result2;
            //    model.LongId = values[1].Cast<int>();
            //    model.Name = values[3].Cast<string>();
            //    model.ShortId = (short)values[2].Cast<int>();
            //    if (DateTime.TryParse(values[10].Cast<string>(), out var result1))
            //        model.Timestamp = result1;
            //    model.bindata = values[13] as byte[];
            //    return model;
            //});

            base.Setup();

            var ds = dc.GetDataSource() as SQLite_DataSource;

            //            var sql = @"
            //insert into dbo_TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59','31ada97d-719a-4340-89ad-fd29d0d0a017',x'01020304';
            //insert into dbo_TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 2,2,'TestModel2','b',1,1,1.1,1.2,1.3,'2024-03-01','01:01:01','b6237194-406f-41db-8328-4f8ed82c2a5c',x'FFFFFFFF';
            //insert into dbo_TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select NULL,NULL,'TestModel3','b',1,1,1.1,1.3,1.4,'2024-04-01','01:01:01','b6237194-406f-41db-8328-4f8ed82c2a5d',x'0000FF00';

            //insert into dbo_TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra1',1,1.1,1.2,'2024-01-01',1;
            //insert into dbo_TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra2',1,1.1,1.2,'2023-01-01',2;

            //insert into dbo_TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild1',1,1.1,1.2,'2024-01-01',1,1;
            //insert into dbo_TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild2',1,1.15,1.25,'2023-01-01',3,2;

            //insert into dbo_TestModelGuidParent(id,name) select '31ada97d-719a-4340-89ad-fd29d0d0a017', 'parent1';
            //insert into dbo_TestModelGuidParent(id,name) select '31ada97d-719a-4340-89ad-fd29d0d0a018', 'parent2';

            //insert into dbo_TestModelGuidChild(id,name,parent_id) select '31ada97d-719a-4340-89ad-fd29d0d0a020', 'child1', '31ada97d-719a-4340-89ad-fd29d0d0a017';
            //insert into dbo_TestModelGuidChild(id,name,parent_id) select '31ada97d-719a-4340-89ad-fd29d0d0a021', 'child2', '31ada97d-719a-4340-89ad-fd29d0d0a018';

            //insert into dbo_TestModelNoUnique (id,name) select 1,'a';
            //insert into dbo_TestModelNoUnique (id,name) select 2,'b';
            //insert into dbo_TestModelNoUnique (id,name) select 1,'c';
            //insert into dbo_TestModelNoUnique (id,name) select 3,'b';
            //insert into dbo_TestModelNoUnique (id,name) select null,'d';

            //";

            //            sql += @"
            //insert into dbo_TestModelParentCompositePrimaryKey(i,j,k) select 1,1,'abc';
            //insert into dbo_TestModelParentCompositePrimaryKey(i,j,k) select 1,2,'def';
            //insert into dbo_TestModelParentCompositePrimaryKey(i,j,k) select 2,2, 'ghi';
            //insert into dbo_TestModelParentCompositePrimaryKey(i,j,k) select 3,3, 'jkl';
            //";

            //            sql += @"
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 1,1,1,1,'abc';
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 2,2,1,1,'def';
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 1,2,1,2,'ghi';
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 2,3,2,2,'jkl';
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 3,3,3,3,'mno';
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 4,1,null,null,'pqr';
            //insert into dbo_TestModelChildCompositePrimaryKey(i,j,Parent_i,Parent_j,k) select 5,5,1,2,'stu';
            //";

            //            sql += @"
            //insert into dbo_TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 1,1,'abc';
            //insert into dbo_TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 2,2,'def';
            //insert into dbo_TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 1,2,'ghi';
            //insert into dbo_TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 2,3,'jkl';
            //insert into dbo_TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 3,3,'mno';
            //insert into dbo_TestModelPrimaryKeyAsForeignKey(Child_i,Child_j,k) select 4,1,'pqr';
            //";

            //            sql += @"
            //insert into dbo_TestModelCompositePrimaryKey(i,j,k) select 1,1,'abc';
            //insert into dbo_TestModelCompositePrimaryKey(i,j,k) select 2,2,'def';
            //insert into dbo_TestModelCompositePrimaryKey(i,j,k) select 1,2,'ghi';
            //insert into dbo_TestModelCompositePrimaryKey(i,j,k) select 2,3,'jkl';
            //insert into dbo_TestModelCompositePrimaryKey(i,j,k) select 3,3,'mno';
            //insert into dbo_TestModelCompositePrimaryKey(i,j,k) select 4,1,'pqr';
            //";
            //            ds.Execute(sql);
        }
    }
}

