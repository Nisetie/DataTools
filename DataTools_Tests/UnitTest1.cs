using DataTools.Attributes;
using DataTools.Common;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.InMemory_SQLite;
using DataTools.MSSQL;
using DataTools.PostgreSQL;
using DataTools.SQLite;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data.SQLite;

namespace DataTools_Tests
{
    public abstract class CommonTests<ContextT> where ContextT : DataContext, new()
    {
        public ContextT DataContext;
        public string alias;

        public CommonTests(string alias)
        {
            DataManager.AddContext(alias, DataContext = new ContextT());
        }

        [Category("Select")]
        [Test]
        public void TestSelectNoException()
        {
            Assert.DoesNotThrow(() => DataContext.Select<TestModelChild>().ToList());
        }

        [Category("Select")]
        [Test]
        public void TestSelectWithoutException()
        {
            var result = DataContext.SelectFrom<TestModelChild>().OrderBy("Name").Select();
        }

        [Category("Select")]
        [Test]
        public void TestSelectWithParameter()
        {
            var par = new SqlParameter("par1");
            var query = DataContext.SelectFrom<TestModelChild>().Where(new SqlWhereClause().AndName("Name").Eq(par));

            par.Value = "TestModelChild";

            TestContext.Out.WriteLine(query.ToString());

            var result = query.Select(par);

            int a = result.Count();

            par.Value = "TestModelChild1";

            result = query.Select(par);

            int b = result.Count();

            Assert.That(a == 0 && b == 1);
        }

        [Category("Select")]
        [Test]
        public void TestSelectDateTime()
        {
            DateTime now = DateTime.Now;
            DateTime result = DateTime.Now;
            if (DataContext is PostgreSQL_DataContext)
                result = (DateTime)DataContext.ExecuteScalar(new SqlSelect().Select(new SqlConstant(now)));
            else if (DataContext is MSSQL_DataContext)
                result = (DateTime)DataContext.ExecuteScalar(new SqlSelect().Select(new SqlComposition(new SqlCustom("cast ("), new SqlConstant(now), new SqlCustom(" as datetime)"))));
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive("Unsupported");
            else
                result = DateTime.Parse(DataContext.ExecuteScalar(new SqlSelect().Select(new SqlConstant(now))).ToString());
            Assert.That(
                now.Year == result.Year
                && now.Month == result.Month
                && now.Day == result.Day
                && now.Hour == result.Hour
                && now.Minute == result.Minute
                && now.Second == result.Second
                );
        }

        [Category("Select")]
        [Test]
        public void TestSelectDateTimeOffset()
        {
            DateTimeOffset now = DateTimeOffset.Now;
            DateTimeOffset result = DateTimeOffset.Now;
            if (DataContext is PostgreSQL_DataContext)
                result = new DateTimeOffset((DateTime)DataContext.ExecuteScalar(new SqlSelect().Select(new SqlConstant(now))));
            else if (DataContext is MSSQL_DataContext)
                result = (DateTimeOffset)DataContext.ExecuteScalar(new SqlSelect().Select(new SqlComposition(new SqlCustom("cast ("), new SqlConstant(now), new SqlCustom(" as datetimeoffset)"))));
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive("Unsupported");
            else
                result = DateTimeOffset.Parse(DataContext.ExecuteScalar(new SqlSelect().Select(new SqlConstant(now))).ToString());

            now = now.UtcDateTime;
            result = result.UtcDateTime;

            Assert.That(
                now.Year == result.Year
                && now.Month == result.Month
                && now.Day == result.Day
                && now.Hour == result.Hour
                && now.Minute == result.Minute
                && now.Second == result.Second
                && now.Offset == result.Offset
                );
        }

        [Category("Select")]
        [Test]
        public void TestSelectLessOrEqual()
        {
            var result = DataContext.SelectFrom<TestModelChild>().Where(new SqlWhereClause().AndName("FValue").LeValue(1.1F)).OrderBy("Name").Select();
            Assert.That(result.Count() == 1);
        }

        [Category("Select")]
        [Test]
        public void TestSelectMany()
        {
            var result = DataContext.SelectFrom<TestModel>().OrderBy("Name").Select().ToArray();
            Assert.That(
                (
                result.Length == 3 &&

                result[0].Id == 1 &&
                result[0].LongId == 1L &&
                result[0].ShortId == 1 &&
                result[0].Name == "TestModel1" &&
                result[0].CharCode == "a" &&
                result[0].Checked == false &&
                result[0].Value == 1 &&
                result[0].FValue == 1.1F &&
                result[0].GValue == 1.2 &&
                result[0].Money == (decimal)1.3 &&
                result[0].Timestamp == DateTime.Parse("2024-01-01") &&
                result[0].Duration == TimeSpan.Parse("23:59:59") &&
                Convert.ToHexString(result[0].bindata) == "01020304"
                )
                &&
               (
                result[1].Id == 2 &&
                result[1].LongId == 2L &&
                result[1].ShortId == 2 &&
                result[1].Name == "TestModel2" &&
                result[1].CharCode == "b" &&
                result[1].Checked == true &&
                result[1].Value == 1 &&
                result[1].FValue == 1.1F &&
                result[1].GValue == 1.2 &&
                result[1].Money == (decimal)1.3 &&
                result[1].Timestamp == DateTime.Parse("2024-03-01") &&
                result[1].Duration == TimeSpan.Parse("01:01:01") &&
                Convert.ToHexString(result[1].bindata) == "FFFFFFFF"
               )
               &&
                 (
                result[2].Id == 3 &&
                result[2].LongId == null &&
                result[2].ShortId == null &&
                result[2].Name == "TestModel3" &&
                result[2].CharCode == "b" &&
                result[2].Checked == true &&
                result[2].Value == 1 &&
                result[2].FValue == 1.1F &&
                result[2].GValue == 1.3 &&
                result[2].Money == (decimal)1.4 &&
                result[2].Timestamp == DateTime.Parse("2024-04-01") &&
                result[2].Duration == TimeSpan.Parse("01:01:01") &&
                Convert.ToHexString(result[2].bindata) == "0000FF00"
               )
            );
        }

        [Category("Select")]
        [Test]
        public void TestSelectMany1()
        {
            var result = DataContext.SelectFrom<TestModelChild>().OrderBy("Name").Select().ToArray();
            Assert.That(
                (
                result.Length > 0 &&

                result[0].Id == 1 &&
                result[0].Name == "TestModelChild1" &&
                result[0].FValue == 1.1F &&
                result[0].GValue == 1.2 &&
                result[0].Timestamp.Value.ToString("yyyy-MM-dd") == "2024-01-01" &&
                result[0].Parent.Id == 1 &&
                result[0].Extra.Id == 1
                )
                &&
               (
               result[1].Id == 2 &&
                result[1].Name == "TestModelChild2" &&
                result[1].FValue == 1.15F &&
                result[1].GValue == 1.25 &&
                result[1].Timestamp.Value.ToString("yyyy-MM-dd") == "2023-01-01" &&
                result[1].Parent.Id == 3 &&
                result[1].Extra.Id == 2
               )
            );
        }

        [Category("Select")]
        [TestCase(1, ExpectedResult = 1)]
        [TestCase(0, ExpectedResult = 0)]
        public int TestWhere(int id)
        {
            var ar = DataContext.SelectFrom<TestModelChild>().Where("Id", id).Select().ToArray();
            return ar.Length;
        }

        [Category("Select")]
        [Test]
        public void TestWhereIsNull()
        {
            var ar = DataContext.SelectFrom<TestModelSimple>().Where("Id", null).Select().ToArray();
            TestContext.Out.WriteLine($"{ar.Length} {string.Join<TestModelSimple>(',',ar)}"); ;
            Assert.That(ar.Length == 1);
        }

        [Category("Insert")]
        [TestCase]
        public void TestInsert()
        {
            if (DataContext is InMemory_SQLite_DataContext) TestInsertInMemory();
            else TestInsertRDBMS();

        }

        [Category("Insert")]
        [TestCase]
        public void TestInsert1()
        {
            if (DataContext is InMemory_SQLite_DataContext) TestInsertInMemory1();
            else TestInsertRDBMS1();

        }

        public void TestInsertInMemory()
        {

            var count = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChild>()).Count();

            var m = new TestModelChild()
            {
                Id = 1,
                Name = $"{nameof(TestModelChild)}",
                Extra = null,
                FValue = float.MaxValue,
                GValue = double.MaxValue,
                Parent = null,
                Timestamp = DateTime.MinValue,
                Value = int.MinValue
            };
            DataContext.Insert(m);

            var newCount = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChild>()).Count();
            TestContext.Out.WriteLine($"old count {count}; new count {newCount}");
            Assert.That(count + 1 == newCount);
        }

        public void TestInsertInMemory1()
        {

            var count = DataContext.ExecuteWithResult(new SqlSelect().From<TestModel>()).Count();

            var m = new TestModel()
            {
                Id = 4,
                LongId = 5,
                ShortId = 6,
                Name = $"{nameof(TestModel)}4",
                CharCode = "c",
                Checked = true,
                Value = int.MinValue,
                FValue = float.MaxValue,
                GValue = double.MinValue,
                Money = (decimal)565.5655,
                Timestamp = DateTime.Parse("2024-03-01"),
                Duration = TimeSpan.Parse("12:12:31"),
                Guid = Guid.NewGuid(),
                bindata = new byte[] { 1, 5, 2, 3 }
            };
            DataContext.Insert(m);

            var newCount = DataContext.ExecuteWithResult(new SqlSelect().From<TestModel>()).Count();

            var inserted = DataContext.SelectFrom<TestModel>().Where("Id", 4).Select().ToArray()[0];

            Assert.That(count + 1 == newCount
                && (
                inserted.Id == m.Id &&
                inserted.LongId == m.LongId &&
                inserted.ShortId == m.ShortId &&
                inserted.Name == m.Name &&
                inserted.CharCode == m.CharCode &&
                inserted.Checked == m.Checked &&
                inserted.Value == m.Value &&
                inserted.FValue == m.FValue &&
                inserted.GValue == m.GValue &&
                inserted.Money == m.Money &&
                inserted.Timestamp == m.Timestamp &&
                inserted.Duration == m.Duration &&
                inserted.Guid == m.Guid &&
                Convert.ToHexString(inserted.bindata) == Convert.ToHexString(m.bindata)
                )

                );
        }
        public void TestInsertRDBMS()
        {
            bool isLong = false;

            var count = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChild>()).Count();
            if (count is long)
                isLong = true;

            var m = new TestModelChild()
            {
                Name = $"{nameof(TestModelChild)}",
                Extra = null,
                FValue = float.MaxValue,
                GValue = double.MaxValue,
                Parent = null,
                Timestamp = DateTime.MinValue,
                Value = int.MinValue
            };
            DataContext.Insert(m);

            var newCount = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChild>()).Count();

            if (isLong)
                Assert.That((long)count + 1 == (long)newCount);
            else
                Assert.That((int)count + 1 == (int)newCount);
        }

        public void TestInsertRDBMS1()
        {
            var count = DataContext.ExecuteWithResult(new SqlSelect().From<TestModel>()).Count();

            var m = new TestModel()
            {
                Id = 4,
                LongId = 5,
                ShortId = 6,
                Name = $"{nameof(TestModel)}4",
                CharCode = "c",
                Checked = true,
                Value = int.MinValue,
                FValue = float.MaxValue,
                GValue = double.MinValue,
                Money = (decimal)565.5655,
                Timestamp = DateTime.Parse("2024-03-01"),
                Duration = TimeSpan.Parse("12:12:31"),
                Guid = Guid.NewGuid(),
                bindata = new byte[] { 1, 5, 2, 3 }
            };
            DataContext.Insert(m);

            var newCount = DataContext.ExecuteWithResult(new SqlSelect().From<TestModel>()).Count();

            var inserted = DataContext.SelectFrom<TestModel>().Where("Id", 4).Select().ToArray()[0];

            Assert.That(count + 1 == newCount
                && (
                inserted.Id == m.Id &&
                inserted.LongId == m.LongId &&
                inserted.ShortId == m.ShortId &&
                inserted.Name == m.Name &&
                inserted.CharCode == m.CharCode &&
                inserted.Checked == m.Checked &&
                inserted.Value == m.Value &&
                inserted.FValue == m.FValue &&
                inserted.GValue == m.GValue &&
                inserted.Money == m.Money &&
                inserted.Timestamp == m.Timestamp &&
                inserted.Duration == m.Duration &&
                inserted.Guid == m.Guid &&
                Convert.ToHexString(inserted.bindata) == Convert.ToHexString(m.bindata)
                )

                );
        }

        [Category("Update")]
        [TestCase(arg: 1)]
        [TestCase(arg: 0)]
        public void TestUpdate(int id)
        {
            var r = DataContext.SelectFrom<TestModelChild>().Where("Id", id).Select().ToArray();

            if (r.Count() == 1)
                Assert.That(true);
            else
                Assert.Pass("no model found");

            var m = r.ToArray()[0];
            m.Name = "NewName";

            DataContext.Update(m);

            var n = DataContext.SelectFrom<TestModelChild>().Where("Id", id).Select().ToArray()[0];
            var nn = DataContext.SelectFrom<TestModelChild>().Where(new SqlWhereClause().AndName("Id").NeValue(id)).Select().ToArray();

            Assert.That(n.Name == "NewName" && nn.All(m => m.Name != "NewName"));
        }

        //[Category("Update")]
        //[Test]
        //public void TestUpdateWithNoUnique1()
        //{
        //    var result = Context.SelectFrom<TestModelNoUnique>().Where("Id", 3).Execute().ToArray()[0];

        //    result.Name = "Noname";

        //    Context.Update(result);

        //    Assert.That(Context.SelectFrom<TestModelNoUnique>().Execute().Count(r => r.Name == result.Name) == 1);
        //}

        //[Category("Update")]
        //[Test]
        //public void TestUpdateWithNoUnique2()
        //{
        //    var result = Context.SelectFrom<TestModelNoUnique>().Where("Id", 1).Execute().ToArray()[0];

        //    result.Name = "Noname";

        //    Context.Update(result);

        //    Assert.That(Context.SelectFrom<TestModelNoUnique>().Execute().Count(r => r.Name == result.Name) == 2);
        //}

        [Category("Update")]
        [Test]
        public void TestUpdateBinary()
        {
            var result = DataContext.SelectFrom<TestModel>().Where("Id", 1).Select().ToArray();
            TestContext.Out.WriteLine(JsonConvert.SerializeObject(result));
            var s = Convert.ToHexString(new byte[] { 255, 255, 0, 255 });

            result[0].bindata = new byte[] { 255, 255, 0, 255 };

            DataContext.Update(result[0]);

            result = DataContext.Select<TestModel>(new SqlSelect().From<TestModel>().Where("Id", 1)).ToArray();

            Assert.That(s == Convert.ToHexString(result[0].bindata));
        }

        [Category("Procedure")]
        [Test]
        public void TestCallProcedureCreating()
        {
            if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive("Unsupported");
            if (DataContext is SQLite_DataContext)
                Assert.Inconclusive("Unsupported");

            var q = new SqlProcedure().Call("test");

            var ds = DataContext.GetDataSource();

            WriteSQLQuery(q);

            q = new SqlProcedure().Call("test").Parameter(new SqlConstant(1), new SqlConstant("abc"));

            WriteSQLQuery(q);
        }

        [Category("Random")]
        private void WriteSQLQuery(SqlExpression q)
        {
            var ds = DataContext.GetDataSource();
            switch (ds)
            {
                case MSSQL_DataSource mssql_ds:
                    TestContext.Out.WriteLine(new MSSQL_QueryParser().ToString(q));
                    break;
                case PostgreSQL_DataSource psql_ds:
                    TestContext.Out.WriteLine(new PostgreSQL_QueryParser().ToString(q));
                    break;
                case SQLite_DataSource sqlite_ds:
                    TestContext.Out.WriteLine(new SQLite_QueryParser().ToString(q));
                    break;
                default:
                    TestContext.Out.WriteLine(q.ToString());
                    Assert.Inconclusive("Unsupported");
                    break;
            }
        }

        [Category("Procedure")]
        [Test]
        public void TestCallProcedureWithoutParamAndReturns()
        {
            var context = DataManager.GetContext(alias);

            if (context is InMemory_SQLite_DataContext) Assert.Inconclusive("Unsupported");

            switch (context)
            {
                case MSSQL_DataContext:
                    break;
                case PostgreSQL_DataContext:
                    break;
                case SQLite_DataContext:
                    Assert.Pass($"{nameof(SQLite_DataContext)} does not support procedures");
                    break;
                default:
                    Assert.Fail();
                    break;
            }
            ;

            context.CallProcedure("dbo.testProc");
        }

        [Category("Select")]
        [TestCase]
        public void TestSelectCreating()
        {
            var q = new SqlSelect()
                .From(
                    new SqlSelect()
                    .Select("a", "b", "c")
                    .From("test")
                    .Where(new SqlWhereClause().AndName("a").EqValue(1).AndName("b").EqValue(2).OrName("c").EqValue(3))
                    , "t"
                    )
                .Select(new SqlCustom("count(*)"));

            WriteSQLQuery(q);

            Assert.Pass();
        }

        [Category("Insert")]
        [TestCase]
        public void TestInsertCreating()
        {
            var q = new SqlInsert()
                .Into("test")
                .Column("a", "b", "c")
                .Value(1, 2, 3);

            WriteSQLQuery(q);

            q = new SqlInsert()
              .Into("test")
              .Column("a", "b", "c")
              .Value("a", DateTime.Now, 232.4343);

            WriteSQLQuery(q);

            q = new SqlInsert()
              .Into("test")
              .Column("a", "b", "c")
              .Value(new SqlSelect().From(new SqlName("test1")).Select(new SqlName("a")).Where(new SqlWhereClause(new SqlName("b")).Eq(new SqlConstant(1))), new SqlCustom("select count() from sys.tables"), new SqlConstant(null));



            WriteSQLQuery(q);
        }

        [Category("Update")]
        [Test]
        public void TestUpdateCreating()
        {
            var q = new SqlUpdate()
                .From("test")
                .Set("a", "b", "c")
                .Value(1, "a", new SqlCustom("(select count(*) from sys.tables)"))
                .Where(
                    new SqlWhereClause()
                    .And(new SqlName("a"))
                    .EqValue(5)
                    .And(
                        new SqlWhereClause()
                        .AndName("b")
                        .EqValue("ds")
                        .OrName("c")
                        .EqValue(Guid.NewGuid())
                    )
                );
            WriteSQLQuery(q);


            q = new SqlUpdate().From("test").Set("a").Value(1).Where(new SqlWhereClause().And(new SqlWhereClause().AndName("a").EqValue(5)).Or(new SqlWhereClause().AndName("a").LtValue(1)));

            WriteSQLQuery(q);
        }

        [Category("Delete")]
        [Test]
        public void TestDeleteCreating()
        {
            var q = new SqlDelete()
                .From("test")
                .Where(new SqlWhereClause().AndName("a").EqValue(1).AndName("b").EqValue(2).OrName("c").EqValue(3));
            WriteSQLQuery(q);
        }

        [Category("Delete")]
        [Test]
        public void TestDeleteAllFromTable()
        {
            var q = new SqlDelete().From<TestModelChild>();
            DataContext.Execute(q);

            var ar = DataContext.Select<TestModelChild>().ToArray();

            Assert.That(ar.Length == 0);
        }

        [Category("Delete")]
        [Test]
        public void TestDeleteSingleRowFromTable()
        {
            var q = new SqlDelete().From<TestModelChild>().Where(nameof(TestModelChild.Name), "TestModelChild1");
            DataContext.Execute(q);
            var l1 = DataContext.Select<TestModelChild>().ToArray().Length;

            DataContext.Delete(DataContext.Select<TestModelChild>().ToArray()[0]);

            var l2 = DataContext.Select<TestModelChild>().ToArray().Length;

            Assert.That(l1 == 1 && l2 == 0);
        }

        [Category("Function")]
        [Test]
        public void TestCallFunctionCreating()
        {
            var q = new SqlFunction().Call("now");
            WriteSQLQuery(q);

            q = new SqlFunction().Call("upper").Parameter(new SqlConstant(1), new SqlConstant("abc"));
            WriteSQLQuery(q);

            var s = new SqlSelect().Select(new SqlFunction().Call("now"));
            WriteSQLQuery(s);

            s = new SqlSelect().Select(new SqlFunction().Call("coalesce").Parameter(new SqlConstant(1), new SqlConstant("abc")));
            WriteSQLQuery(s);
        }

        [Category("Function")]
        [Test]
        public void TestCallTableFunctionWithoutParam()
        {
            switch (DataContext)
            {
                case SQLite_DataContext:
                    Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support stored table functions");
                    break;
                case InMemory_SQLite_DataContext:
                    Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support stored table functions");
                    break;
            }

            var result = DataContext.CallTableFunction<TestModel>("dbo.testFunc");

            Assert.That(result.Count() == 3);
        }

        [Category("Function")]
        [TestCase(arguments: 0)]
        [TestCase(arguments: 1)]
        public void TestCallTableFunctionWithParam(int i)
        {
            switch (DataContext)
            {
                case SQLite_DataContext:
                    Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support stored table functions");
                    break;
                case InMemory_SQLite_DataContext:
                    Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support stored table functions");
                    break;
            }

            var result = DataContext.CallTableFunction<TestModel>("dbo.testFuncParam", i);

            Assert.That(i == result.Count());
        }

        [Category("Function")]
        [Test]
        public void TestCallScalarFunction()
        {
            if (DataContext is InMemory_SQLite_DataContext) Assert.Inconclusive("Unsupported");

            string fName = null;

            switch (DataContext)
            {
                case SQLite_DataContext:
                    fName = "datetime";
                    break;
                case MSSQL_DataContext:
                    fName = "getdate";
                    break;
                case PostgreSQL_DataContext:
                    fName = "now";
                    break;
                default:
                    Assert.Fail();
                    break;
            }

            var result = DataContext.CallScalarFunction(fName);
            if (result is string resultStr)
                Assert.That(DateTime.TryParse(resultStr, out _));
            else if (result is DateTime)
                Assert.Pass();
        }

        [Category("Function")]
        [Test]
        public void TestCallScalarFunctionWithParam()
        {
            if (DataContext is InMemory_SQLite_DataContext) Assert.Inconclusive("Unsupported");

            string fName = null;

            switch (DataContext)
            {
                case SQLite_DataContext:
                    fName = "length";
                    break;
                case MSSQL_DataContext:
                    fName = "len";
                    break;
                case PostgreSQL_DataContext:
                    fName = "length";
                    break;
                default:
                    Assert.Inconclusive("Unsupported");
                    break;
            }

            var result = DataContext.CallScalarFunction(fName, "abc");
            if (result is string resultStr)
                Assert.That(int.TryParse(resultStr, out int resultInt) && resultInt == 3);
            else if (result is int)
                Assert.That(((int)result) == 3);
            else if (result is long)
                Assert.That(((long)result) == 3);
        }

        [NoUnique]
        private class testProcReturn_Result
        {
            public string recs { get; set; }
        }

        [Category("Procedure")]
        [TestCase(arguments: 0)]
        [TestCase(arguments: 1)]
        public void TestCallProcedureWithParamAndReturns(int i)
        {
            var context = DataManager.GetContext(alias);

            if (context is PostgreSQL_DataContext)
                Assert.Inconclusive($"{nameof(PostgreSQL_DataContext)} does not support procedures with result set.");
            if (context is SQLite_DataContext)
                Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support procedures with result set.");
            if (context is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support procedures with result set.");

            var result = context.CallProcedure<testProcReturn_Result>("dbo.testProcReturn", i).ToArray();
            Assert.That(i == result.Length || string.IsNullOrEmpty(result[0].recs));
        }

        [Category("Select")]
        [Test]
        public void TestSelectGUIDWithoutException()
        {
            var result = DataContext.Select<TestModelGuidChild>().ToArray();
            Assert.Pass();
        }

        [Category("Insert")]
        [Test]
        public void TestInsertGUID()
        {

            var a = new TestModelGuidChild();
            var g = Guid.NewGuid();
            a.Id = g;
            a.Name = "child3";
            DataContext.Insert(a);

            var r = DataContext.Select<TestModelGuidChild>(new SqlSelect().From<TestModelGuidChild>().Where("Id", g)).ToArray();

            Assert.That(r.Length == 1 && r[0].Id == g);
        }
    }

    [TestFixture("sqlite", "Data Source=dbo")]
    public class SQLiteTests : CommonTests<SQLite_DataContext>
    {
        SQLiteConnection _conn;

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

        [SetUp]
        public void Setup()
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
            var ds = dc.GetDataSource() as SQLite_DataSource;

            var sql = @"

drop table if exists dbo_TestModel;
CREATE TABLE dbo_TestModel (
    Id INTEGER PRIMARY KEY,
    LongId int,
    ShortId int,
    Name text,
    CharCode text,
    Checked int,
    Value INT,
    FValue REAL,
    GValue real,
    Money real,
    Timestamp text,
    Duration text,
    Guid text,
    bindata blob
);

drop table if exists dbo_TestModelChild;
CREATE TABLE dbo_TestModelChild (
    Id INTEGER PRIMARY KEY,
    Name text,
    Value INT,
    FValue REAL,
    GValue real,
    Timestamp text,
    Parent_id INT,
    Extra_id INT            
);

drop table if exists dbo_TestModelExtra;
CREATE TABLE dbo_TestModelExtra (
    Id INTEGER PRIMARY KEY,
    Name text,
    Value INT,
    FValue REAL,
    GValue real,
    Timestamp text,
    Extra_id INT            
);

drop table if exists dbo_TestModelGuidChild;
drop table if exists dbo_TestModelGuidParent;
CREATE TABLE dbo_TestModelGuidParent (
    Id uuid primary key,
    Name text
);
CREATE TABLE dbo_TestModelGuidChild (
    Id uuid primary key,
    Name text,
    parent_id uuid references TestModelGuidParent(id)
);

drop table if exists dbo_TestModelNoUnique;
create table dbo_TestModelNoUnique (id int, Name text);

insert into dbo_TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59','31ada97d-719a-4340-89ad-fd29d0d0a017',x'01020304';
insert into dbo_TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 2,2,'TestModel2','b',1,1,1.1,1.2,1.3,'2024-03-01','01:01:01','b6237194-406f-41db-8328-4f8ed82c2a5c',x'FFFFFFFF';
insert into dbo_TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select NULL,NULL,'TestModel3','b',1,1,1.1,1.3,1.4,'2024-04-01','01:01:01','b6237194-406f-41db-8328-4f8ed82c2a5d',x'0000FF00';

insert into dbo_TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra1',1,1.1,1.2,'2024-01-01',1;
insert into dbo_TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra2',1,1.1,1.2,'2023-01-01',2;

insert into dbo_TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild1',1,1.1,1.2,'2024-01-01',1,1;
insert into dbo_TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild2',1,1.15,1.25,'2023-01-01',3,2;

insert into dbo_TestModelGuidParent(id,name) select '31ada97d-719a-4340-89ad-fd29d0d0a017', 'parent1';
insert into dbo_TestModelGuidParent(id,name) select '31ada97d-719a-4340-89ad-fd29d0d0a018', 'parent2';

insert into dbo_TestModelGuidChild(id,name,parent_id) select '31ada97d-719a-4340-89ad-fd29d0d0a020', 'child1', '31ada97d-719a-4340-89ad-fd29d0d0a017';
insert into dbo_TestModelGuidChild(id,name,parent_id) select '31ada97d-719a-4340-89ad-fd29d0d0a021', 'child2', '31ada97d-719a-4340-89ad-fd29d0d0a018';

insert into dbo_TestModelNoUnique (id,name) select 1,'a';
insert into dbo_TestModelNoUnique (id,name) select 2,'b';
insert into dbo_TestModelNoUnique (id,name) select 1,'c';
insert into dbo_TestModelNoUnique (id,name) select 3,'b';
insert into dbo_TestModelNoUnique (id,name) select null,'d';

";
            ds.Execute(sql);
        }
    }

    [TestFixture("postgresql", "Username=postgres;Password=12345678;Host=localhost")]
    public class PostgreSQLTests : CommonTests<PostgreSQL_DataContext>
    {
        public PostgreSQLTests(string alias, string connectionString) : base(alias)
        {
            this.alias = alias;
            DataContext.ConnectionString = connectionString;

        }

        [SetUp]
        public void Setup()
        {
            var dc = DataManager.GetContext(alias) as PostgreSQL_DataContext;
            var ds = dc.GetDataSource() as PostgreSQL_DataSource;

            var sql = @"

drop table if exists dbo.TestModel cascade;
CREATE TABLE dbo.TestModel (
    Id int primary key generated always as identity,
    LongId bigint,
    ShortId smallint,
    Name text,
    CharCode char,
    Checked bool,
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Money decimal,
    Timestamp timestamp,
    Duration time,
    Guid uuid,
    bindata bytea
);

drop table if exists dbo.TestModelChild cascade;
CREATE TABLE dbo.TestModelChild (
    Id INT primary key generated always as identity,
    Name text,
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Timestamp timestamp,
    Parent_id INT,
    Extra_id INT            
);

drop table if exists dbo.TestModelExtra cascade;
CREATE TABLE dbo.TestModelExtra (
    Id INT primary key generated always as identity,
    Name text,
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Timestamp timestamp,
    Extra_id INT            
);

drop table if exists dbo.TestModelGuidChild cascade;
drop table if exists dbo.TestModelGuidParent cascade;
CREATE TABLE dbo.TestModelGuidParent (
    Id uuid primary key,
    Name text
);
CREATE TABLE dbo.TestModelGuidChild (
    Id uuid primary key,
    Name text,
    parent_id uuid references dbo.TestModelGuidParent(id)
);

drop table if exists dbo.TestModelNoUnique;
create table dbo.TestModelNoUnique (id int, Name text);


insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 1,1,'TestModel1','a',false,1,1.1,1.2,1.3,'2024-01-01','23:59:59',gen_random_uuid(),'\x01020304';
insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 2,2,'TestModel2','b',true,1,1.1,1.2,1.3,'2024-03-01','01:01:01',gen_random_uuid(),'\xFFFFFFFF';
insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select NULL,NULL,'TestModel3','b',true,1,1.1,1.3,1.4,'2024-04-01','01:01:01',gen_random_uuid(),'\x0000FF00';

insert into dbo.TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra1',1,1.1,1.2,'2024-01-01',1;
insert into dbo.TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra2',1,1.1,1.2,'2023-01-01',2;

insert into dbo.TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild1',1,1.1,1.2,'2024-01-01',1,1;
insert into dbo.TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild2',1,1.15,1.25,'2023-01-01',3,2;

insert into dbo.TestModelGuidParent(id,name) select gen_random_uuid(), 'parent1';
insert into dbo.TestModelGuidParent(id,name) select gen_random_uuid(), 'parent2';

insert into dbo.TestModelGuidChild(id,name,parent_id) select gen_random_uuid(), 'child1', (select id from dbo.TestModelGuidParent where name = 'parent1');
insert into dbo.TestModelGuidChild(id,name,parent_id) select gen_random_uuid(), 'child2', (select id from dbo.TestModelGuidParent where name = 'parent2');

insert into dbo.TestModelNoUnique (id,name) select 1,'a';
insert into dbo.TestModelNoUnique (id,name) select 2,'b';
insert into dbo.TestModelNoUnique (id,name) select 1,'c';
insert into dbo.TestModelNoUnique (id,name) select 3,'b';
insert into dbo.TestModelNoUnique (id,name) select null,'d';


";
            ds.Execute(sql);


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
    }

    [TestFixture("mssql", "Data Source=localhost;Integrated Security=True;Pooling=True")]
    public class MSSQLTests : CommonTests<MSSQL_DataContext>
    {

        public MSSQLTests(string alias, string connectionString) : base(alias)
        {
            this.alias = alias;
            DataContext.ConnectionString = connectionString;
        }

        [SetUp]
        public void SetUp()
        {
            var dc = DataManager.GetContext(alias) as MSSQL_DataContext;
            var ds = dc.GetDataSource() as MSSQL_DataSource;

            var sql = @"
drop table if exists dbo.TestModelChild;
drop table if exists dbo.TestModel;
drop table if exists dbo.TestModelExtra;

drop table if exists dbo.TestModelGuidChild;
drop table if exists dbo.TestModelGuidParent;

drop table if exists dbo.TestModelNoUnique;
";

            ds.Execute(sql);

            sql = @"
CREATE TABLE dbo.TestModel (
    Id int primary key IDENTITY (1, 1),
    LongId bigint,
    ShortId smallint,
    Name NVARCHAR (MAX),
    CharCode char,
    Checked bit,
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Money money,
    Timestamp DATETIME,
    Duration time,
    Guid uniqueidentifier,
    bindata varbinary(max)
);


CREATE TABLE dbo.TestModelChild (
    Id INT primary key IDENTITY (1, 1),
    Name NVARCHAR (MAX),
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Timestamp DATETIME,
    Parent_id INT,
    Extra_id INT            
);


CREATE TABLE dbo.TestModelExtra (
    Id INT primary key IDENTITY (1, 1),
    Name NVARCHAR(MAX),
    Value INT,
    FValue REAL,
    GValue FLOAT (53),
    Timestamp DATETIME,
    Extra_id INT            
);


CREATE TABLE dbo.TestModelGuidParent (
    Id uniqueidentifier primary key,
    Name NVARCHAR(MAX)
);
CREATE TABLE dbo.TestModelGuidChild (
    Id uniqueidentifier primary key,
    Name NVARCHAR(MAX),
    parent_id uniqueidentifier references TestModelGuidParent(id)
);

create table dbo.TestModelNoUnique (id int, Name nvarchar(max));

";
            ds.Execute(sql);

            sql = @"
insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59',newid(),0x01020304;
insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select 2,2,'TestModel2','b',1,1,1.1,1.2,1.3,'2024-03-01','01:01:01',newid(),0xFFFFFFFF;
insert into dbo.TestModel(LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,bindata) select NULL,NULL,'TestModel3','b',1,1,1.1,1.3,1.4,'2024-04-01','01:01:01',newid(),0x0000FF00;

insert into dbo.TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra1',1,1.1,1.2,'2024-01-01',1;
insert into dbo.TestModelExtra(Name,Value,FValue,GValue,Timestamp,Extra_id) select 'TestModelExtra2',1,1.1,1.2,'2023-01-01',2;

insert into dbo.TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild1',1,1.1,1.2,'2024-01-01',1,1;
insert into dbo.TestModelChild(Name,Value,FValue,GValue,Timestamp,Parent_id,Extra_id) select 'TestModelChild2',1,1.15,1.25,'2023-01-01',3,2;

insert into dbo.TestModelGuidParent(id,name) select newid(), 'parent1';
insert into dbo.TestModelGuidParent(id,name) select newid(), 'parent2';

insert into dbo.TestModelGuidChild(id,name,parent_id) select newid(), 'child1', (select id from dbo.TestModelGuidParent where name = 'parent1');
insert into dbo.TestModelGuidChild(id,name,parent_id) select newid(), 'child1', (select id from dbo.TestModelGuidParent where name = 'parent2');

insert into dbo.TestModelNoUnique (id,name) select 1,'a';
insert into dbo.TestModelNoUnique (id,name) select 2,'b';
insert into dbo.TestModelNoUnique (id,name) select 1,'c';
insert into dbo.TestModelNoUnique (id,name) select 3,'b';
insert into dbo.TestModelNoUnique (id,name) select null,'d';


";
            ds.Execute(sql);

            ds.Execute("drop procedure if exists dbo.testProc");
            ds.Execute("drop procedure if exists dbo.testProcReturn");
            ds.Execute(@"
create procedure dbo.testProc
as begin
    declare @i int;
end;
");

            ds.Execute(@"
create procedure dbo.testProcReturn
@id int
as begin
    select * from dbo.TestModel where id = @id;
end;
");

            ds.Execute("drop function if exists dbo.testFunc");
            ds.Execute(@"
create function dbo.testFunc()
returns table
as
    return (select * from dbo.TestModel);
");

            ds.Execute("drop function if exists dbo.testFuncParam");
            ds.Execute(@"
create function dbo.testFuncParam(@i int)
returns table
as
    return (select * from dbo.TestModel where id = @i);
");
        }


        [Test]
        public void GetMetadata()
        {
            string _query = @"
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
,cast(tablesAndColums.[Generated] as bit)
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
            var result = DataContext.Select<Metadata>(new SqlCustom(_query));
        }
    }


    [TestFixture("inmemory")]
    public class InMemorySQLiteTests : CommonTests<InMemory_SQLite_DataContext>
    {
        public InMemorySQLiteTests(string alias) : base(alias)
        {
            this.alias = alias;
        }

        void Insert<ModelT>(object[] values) where ModelT : class, new()
        {
            var model = ModelMapper<ModelT>.MapModel(DataContext, null, values);
            DataContext.Insert<ModelT>(model);
        }

        [SetUp]
        public void SetUp()
        {
            var dc = DataManager.GetContext(alias) as InMemory_SQLite_DataContext;

            dc.DropTable<TestModel>();
            dc.CreateTable<TestModel>();
            Insert<TestModel>([
                    1,
                    1L,
                    1,
                    "TestModel1",
                    'a',
                    false,
                    1,
                    1.1F,
                    1.2,
                    1.3,
                    DateTime.Parse("2024-01-01"),
                    TimeSpan.Parse("23:59:59"),
                    Guid.NewGuid(),
                    Convert.FromHexString("01020304")
                ]);
            Insert<TestModel>(
                new object[] {
                    2 ,
                    2 ,
                    2 ,
                    "TestModel2" ,
                    'b' ,
                    true ,
                    1,
                    1.1F,
                    1.2,
                    1.3,
                    DateTime.Parse("2024-03-01") ,
                    TimeSpan.Parse("01:01:01"),
                    Guid.NewGuid(),
                    Convert.FromHexString("FFFFFFFF")
                }
            );
            Insert<TestModel>(
                new object[] {
                    3,
                    null ,
                    null ,
                    "TestModel3",
                    'b',
                    true,
                    1,
                    1.1F,
                    1.3,
                    1.4,
                    DateTime.Parse("2024-04-01"),
                    TimeSpan.Parse("01:01:01"),
                    Guid.NewGuid(),
                    Convert.FromHexString("0000FF00")
                }
            );

            dc.DropTable<TestModelExtra>();
            dc.CreateTable<TestModelExtra>();
            Insert<TestModelExtra>(
                new object[] {
                    1,
                    "TestModelExtra1" ,
                     1 ,
                    1.1F,
                    1.2,
                    DateTime.Parse("2024-01-01"),
                    1
                }
            );
            Insert<TestModelExtra>(
                new object[] {
                    2,
                    "TestModelExtra2" ,
                    1,
                    1.1F,
                    1.2,
                    DateTime.Parse("2023-01-01"),
                    2
                }
            );

            dc.DropTable<TestModelChild>();
            dc.CreateTable<TestModelChild>();
            Insert<TestModelChild>(
                new object[] {
                    1,
                    "TestModelChild1" ,
                    1,
                    1.1F,
                    1.2,
                    DateTime.Parse("2024-01-01"),
                    1,
                    1
                }
            );
            Insert<TestModelChild>(
                new object[] {
                   2,
                    "TestModelChild2" ,
                    1,
                    1.15F,
                    1.25,
                    DateTime.Parse("2023-01-01"),
                    3,
                    2
               }
           );

            dc.DropTable<TestModelGuidParent>();
            dc.CreateTable<TestModelGuidParent>();
            var pguid1 = Guid.NewGuid();
            var pguid2 = Guid.NewGuid();
            Insert<TestModelGuidParent>(
                new object[] {
                    pguid1,
                    "parent1"
                }
            );
            Insert<TestModelGuidParent>(
                new object[] {
                    pguid2 ,
                    "parent2"
                }
            );

            dc.CreateTable<TestModelGuidChild>();
            Insert<TestModelGuidChild>(
                new object[] {
                    Guid.NewGuid(),
                    "child1",
                    pguid1
                }
            );
            Insert<TestModelGuidChild>(
                new object[] {
                    Guid.NewGuid() ,
                    "child2",
                    pguid2
                }
            );

            dc.DropTable<TestModelSimple>();
            dc.CreateTable<TestModelSimple>();
            Insert<TestModelSimple>(new object[] { 1, 'a' });
            Insert<TestModelSimple>(new object[] { 2, 'b' });
            Insert<TestModelSimple>(new object[] { 1, 'c' });
            Insert<TestModelSimple>(new object[] { 3, 'b' });
            Insert<TestModelSimple>(new object[] { null, 'd' });
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

    [ObjectName("TestModel", "dbo"), DisplayModelName("Тестовая модель")]
    public class TestModel
    {
        [IgnoreChanges, Unique, Autoincrement]
        public int Id { get; set; }
        public long? LongId { get; set; }
        public short? ShortId { get; set; }
        public string Name { get; set; }
        public string CharCode { get; set; }
        public bool Checked { get; set; }
        public int Value { get; set; }
        public float FValue { get; set; }
        public double GValue { get; set; }
        public decimal Money { get; set; }
        public DateTime Timestamp { get; set; }
        public TimeSpan Duration { get; set; }
        public Guid Guid { get; set; }

        public byte[] bindata { get; set; }

        public override string ToString()
        {
            return Name;
        }
    }

    [ObjectName("TestModelChild", "dbo"), DisplayModelName("Дочерняя тестовая модель")]
    public class TestModelChild
    {
        [IgnoreChanges, Unique, Autoincrement]
        public int Id { get; set; }
        public string? Name { get; set; }
        public int? Value { get; set; }
        public float? FValue { get; set; }
        public double? GValue { get; set; }
        public DateTime? Timestamp { get; set; }
        [Reference(nameof(TestModel.Id)), ColumnName("Parent_id")] public TestModel? Parent { get; set; }
        [Reference(nameof(TestModelExtra.Id)), ColumnName("Extra_id")] public TestModelExtra? Extra { get; set; }

        public override string? ToString()
        {
            return Name;
        }
    }

    [ObjectName("TestModelExtra", "dbo"), DisplayModelName("Дополнительная тестовая модель")]
    public class TestModelExtra
    {
        [IgnoreChanges, Unique, Autoincrement]
        public int Id { get; set; }
        public string? Name { get; set; }
        public int? Value { get; set; }
        public float? FValue { get; set; }
        public double? GValue { get; set; }
        public DateTime? Timestamp { get; set; }

        public override string? ToString()
        {
            return Name;
        }
    }

    [ObjectName("TestModelGuidParent", "dbo")]
    public class TestModelGuidParent
    {
        [Unique]
        public Guid Id { get; set; }
        public string Name { get; set; }
    }

    [ObjectName("TestModelGuidChild", "dbo")]
    public class TestModelGuidChild
    {
        [Unique]
        public Guid Id { get; set; }
        public string Name { get; set; }
        [Reference(nameof(TestModelGuidParent.Id)), ColumnName("parent_id")]
        public TestModelGuidParent Parent { get; set; }
    }

    [ObjectName("TestModelNoUnique", "dbo")]
    [NoUnique]
    public class TestModelSimple
    {
        //[Unique]
        public int? Id { get; set; }
        public string Name { get; set; }
    }


    class Metadata
    {
        [Unique]
        public string TABLE_CATALOG { get; set; }
        [Unique]
        public string TABLE_SCHEMA { get; set; }
        [Unique]
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

