using DataTools.Common;
using DataTools.Deploy;
using DataTools.InMemory_SQLite;
using DataTools.Interfaces;
using DataTools.SQLite;
using NUnit.Framework.Internal;

namespace DataTools_Tests
{
    [TestFixture("inmemory")]
    public class InMemorySQLiteTests : CommonTests<InMemory_SQLite_DataContext>
    {
        public override GeneratorBase GetGenerator()
        {
            throw new NotImplementedException();
        }

        public override IDBMS_QueryParser GetQueryParser()
        {
            return new SQLite_QueryParser();
        }


        public InMemorySQLiteTests(string alias) : base(alias)
        {
            this.alias = alias;
        }

        private void Insert<ModelT>(object[] values) where ModelT : class, new()
        {
            var model = ModelMapper<ModelT>.MapModel(DataContext, null, values, new SelectCache());
            DataContext.Insert<ModelT>(model);
        }
        public override void Setup()
        {
            base.Setup();

            var dc = DataManager.GetContext(alias) as InMemory_SQLite_DataContext;



        }

        //[Test]
        //public void TestSelectPlan()
        //{
        //    var query = new SqlSelect().From<TestModelChild>();
        //    var plans = typeof(InMemory_DataContext).GetField("_plans", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(DataContext) as LinkedList<(string query, Func<InMemory_DataContext, SqlParameter[], IEnumerable<object[]>> plan)>;
        //    DataContext.Select<TestModelChild>(query);
        //    int plansCount = plans.Count;
        //    DataContext.Select<TestModelChild>(query);
        //    Assert.That(plans.Count == plansCount);
        //}
    }
}

