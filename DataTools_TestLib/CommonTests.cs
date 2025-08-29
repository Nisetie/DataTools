using DataTools.Attributes;
using DataTools.Common;
using DataTools.DDL;
using DataTools.Deploy;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.InMemory_SQLite;
using DataTools.Interfaces;
using DataTools.Meta;
using DataTools.MSSQL;
using DataTools.PostgreSQL;
using DataTools.SQLite;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;


namespace DataTools_Tests
{
    public abstract class CommonTests<ContextT> where ContextT : DataContext, new()
    {
        public ContextT DataContext;
        public string alias;

        private static TestData TestData = new TestData();

        public CommonTests(string alias)
        {
            DataManager.AddContext(alias, DataContext = new ContextT());
        }

        public abstract DataTools.Deploy.GeneratorBase GetGenerator();

        /// <summary>
        /// Получить анализатор запросов. Может вернуться NULL, если для конкретной реализации ORM не предусматривается анализ запросов.
        /// </summary>
        /// <returns></returns>
        public abstract IDBMS_QueryParser GetQueryParser();

        public virtual void TeardownScripts() { }

        [NUnit.Framework.TearDown]
        public virtual void Teardown()
        {
            DynamicMapper.ClearMappers();

            TeardownScripts();

            TeardownStatic(DataContext);
        }

        public static void TeardownStatic(IDataContext DataContext)
        {
            DataContext.DropTable<TestModelChild>();
            DataContext.DropTable<TestModel>();
            DataContext.DropTable<TestModelExtra>();

            DataContext.DropTable<TestModelGuidChild>();
            DataContext.DropTable<TestModelGuidParent>();

            DataContext.DropTable<TestModelSimple>();

            DataContext.DropTable<TestModelPrimaryKeyAsForeignKey>();

            DataContext.DropTable<TestModelChildCompositePrimaryKey>();
            DataContext.DropTable<TestModelParentCompositePrimaryKey>();

            DataContext.DropTable<TestModelCompositePrimaryKey>();
        }

        [SetUp]
        public virtual void Setup()
        {
            Teardown();

            SetupStatic(DataContext);
        }

        public static void SetupStatic(IDataContext DataContext)
        {
            DataContext.CreateTable<TestModel>();
            DataContext.CreateTable<TestModelExtra>();
            DataContext.CreateTable<TestModelChild>();

            DataContext.CreateTable<TestModelGuidParent>();
            DataContext.CreateTable<TestModelGuidChild>();

            DataContext.CreateTable<TestModelSimple>();

            DataContext.CreateTable<TestModelCompositePrimaryKey>();

            DataContext.CreateTable<TestModelParentCompositePrimaryKey>();
            DataContext.CreateTable<TestModelChildCompositePrimaryKey>();

            DataContext.CreateTable<TestModelPrimaryKeyAsForeignKey>();

            TestData.InsertTestData(DataContext);
        }

        [Test]
        public void CreateSqlInsertBatch()
        {
            var batch = new SqlInsertBatch();
            batch.Into(new SqlName("test")).Column(new SqlName("i"));


            var ar = new int[100];

            batch.Value(ar.Select(i => new SqlExpression[] { new SqlConstant(i) }).ToArray());

            var parser = GetQueryParser();
            var str = parser.ToString(batch);

            TestContext.Out.WriteLine(str);
        }

        [NoUnique]
        public class GenericWrapper<T>
        {
            public T Value { get; set; }

            public GenericWrapper(T value) { Value = value; }
        }

        public void TestType<T>(T value)
        {
            var wr = new GenericWrapper<T>(value);
            var result = DataContext.Select(ModelMetadata.CreateFromType(wr.GetType()), new SqlComposition(new SqlCustom("SELECT "), new SqlConstant(value), new SqlCustom(" AS Value;"))).First();
            var fromCode = GetQueryParser().SimplifyQuery(new SqlConstant(wr.Value)).ToString();
            var fromResult = GetQueryParser().SimplifyQuery(new SqlConstant(result.Value)).ToString();
            Assert.That(result.Value is T && fromResult == fromCode);
        }

        [Test]
        public void TestTypes()
        {
            TestType(true);
            TestType((int)123456);
            TestType((uint)123456);
            TestType((byte)255);
            TestType((sbyte)127);
            TestType((short)12345);
            TestType((ushort)12345);
            TestType((long)123456);
            TestType((ulong)123456);
            TestType((decimal)125.32m);
            TestType((float)125.32f);
            TestType((double)125.32);
            TestType((string)"abc");
            TestType((char)'a');
            TestType(new byte[] { 1, 2, 3, 4 });
            TestType(DateTime.Parse("2025-08-07 15:27:36"));
            TestType(DateTimeOffset.Parse("2025-08-07 15:27:36"));
            TestType(TimeSpan.Parse("15:27:36"));
            TestType(Guid.Parse("8d5cbb93-28b3-46c0-8c37-e973b3d772a4"));
        }

        [Category("Generator")]
        [Test]
        public void GetDataFromDynamicMetadata()
        {
            if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} unsupported.");


            var generator = GetGenerator();

            var defs = generator.GetModelDefinitions(schemaIncludeNameFilter: "dbo").ToList();

            foreach (var d in defs)
            {
                var r = DataContext.Select(d.ModelMetadata).ToArray();
            }


        }

        [Category("Generator")]
        [Test]
        public void GetMetadata()
        {

            if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} unsupported.");


            var generator = GetGenerator();

            var defs = generator.GetModelDefinitions(schemaIncludeNameFilter: "dbo").ToList();


            var forCreate = new List<ModelDefinition>(defs);

            foreach (var def in from d in defs orderby d.ModelMetadata.Fields.Count(f => f.IsForeignKey) descending select d)
                TestContext.Out.WriteLine(def.ModelCode);


            var alreadyCreated = new List<string>();

            bool isForeign(IModelMetadata foreignModelMetadata, IEnumerable<IModelMetadata> modelMetadatas)
            {
                foreach (var metadata in modelMetadatas)
                    if (metadata.Fields.Any(f => f.IsForeignKey && f.ForeignModel.FullObjectName == foreignModelMetadata.FullObjectName && metadata.FullObjectName != foreignModelMetadata.FullObjectName))
                        return true;
                return false;
            }

            void createRecursively(IModelMetadata modelMetadata)
            {
                if (alreadyCreated.Contains(modelMetadata.FullObjectName))
                    return;
                else
                    foreach (var f in modelMetadata.Fields.Where(f => f.IsForeignKey))
                        if (f.ForeignModel.FullObjectName != modelMetadata.FullObjectName)
                            createRecursively(f.ForeignModel);
                DataContext.CreateTable(modelMetadata);
                alreadyCreated.Add(modelMetadata.FullObjectName);
            }

            // удалить процедуры и функции
            TeardownScripts();

            var forDrop = new List<IModelMetadata>(defs.Select(d => d.ModelMetadata));
            while (forDrop.Count > 0)
            {
                var notForeignFor = forDrop.Where(def => !isForeign(def, forDrop));
                foreach (var def in notForeignFor) DataContext.DropTable(def);
                forDrop.RemoveAll(def => !isForeign(def, forDrop));
            }

            foreach (var def in forCreate)
            {
                createRecursively(def.ModelMetadata);
            }

            TestData.InsertTestData(DataContext);
        }

        [Category("CustomModelMapper")]
        [Test]
        public void TestCustomModelMapper()
        {
            var meta = ModelMetadata<TestModel>.Instance;

            var mapper = new Func<IDataContext, object[], object>((IDataContext context, object[] values) =>
            {
                var m = new TestModel();
                var l = values[meta.GetField(nameof(TestModel.Id)).FieldOrder];
                m.Id = values[meta.GetField(nameof(TestModel.Id)).FieldOrder].Cast<int>();
                m.LongId = values[meta.GetField(nameof(TestModel.LongId)).FieldOrder].Cast<long?>();
                m.ShortId = values[meta.GetField(nameof(TestModel.ShortId)).FieldOrder].Cast<short?>();
                m.Name = values[meta.GetField(nameof(TestModel.Name)).FieldOrder].Cast<string>();
                m.CharCode = values[meta.GetField(nameof(TestModel.CharCode)).FieldOrder].Cast<string>();
                m.Checked = values[meta.GetField(nameof(TestModel.Checked)).FieldOrder].Cast<bool>();
                m.Value = values[meta.GetField(nameof(TestModel.Value)).FieldOrder].Cast<int>();
                m.FValue = values[meta.GetField(nameof(TestModel.FValue)).FieldOrder].Cast<float>();
                m.GValue = values[meta.GetField(nameof(TestModel.GValue)).FieldOrder].Cast<double>();
                m.Money = values[meta.GetField(nameof(TestModel.Money)).FieldOrder].Cast<decimal>();
                m.Timestamp = values[meta.GetField(nameof(TestModel.Timestamp)).FieldOrder].Cast<DateTime>();
                m.Duration = values[meta.GetField(nameof(TestModel.Duration)).FieldOrder].Cast<TimeSpan>();
                m.Guid = values[meta.GetField(nameof(TestModel.Guid)).FieldOrder].Cast<Guid>();
                m.bindata = values[meta.GetField(nameof(TestModel.bindata)).FieldOrder].Cast<byte[]>();

                return m;
            });

            DataContext.AddCustomModelMapper<TestModel>(mapper);

            var result = DataContext.SelectFrom<TestModel>().Select().ToArray();

            DataContext.RemoveCustomModelMapper<TestModel>();
        }

        [Category("Create")]
        [Test]
        public void TestCreateTablePrint()
        {
            var cmd = new SqlCreateTable();
            cmd.Table("dbo.TestCreation").Column(
                new SqlColumnDefinition().Name("i").Type<int>().Constraint(new SqlColumnAutoincrement()),
                new SqlColumnDefinition().Name("j").Type<int>(),
                new SqlColumnDefinition().Name("k").Type<string>(),
                new SqlColumnDefinition().Name("l").Type<Guid>(),
                new SqlColumnDefinition().Name("m").Type<byte[]>()
                );

            TestContext.Out.WriteLine(cmd.ToString());
        }

        [Category("Create")]
        [Test]
        public void TestCreateTablePrint1()
        {
            var cmd = new SqlCreateTable();
            cmd.Table<TestModelChild>();
            TestContext.Out.WriteLine(cmd.ToString());
        }

        [Category("Create")]
        [Test]
        public void TestCreateTable()
        {
            var cmd = new SqlCreateTable();
            cmd.Table("dbo.TestCreation").Column(
                new SqlColumnDefinition().Name("i").Type<int>().Constraint(new SqlColumnAutoincrement()),
                new SqlColumnDefinition().Name("j").Type<int>(),
                new SqlColumnDefinition().Name("k").Type<string>(),
                new SqlColumnDefinition().Name("l").Type<Guid>(),
                new SqlColumnDefinition().Name("m").Type<byte[]>()
                );

            TestContext.Out.WriteLine(cmd.ToString());

            TestContext.Out.WriteLine(GetQueryParser().ToString(cmd));

            TestContext.Out.WriteLine(GetQueryParser().ToString(new SqlCreateTable().Table<TestModelParentCompositePrimaryKey>()));
            TestContext.Out.WriteLine(GetQueryParser().ToString(new SqlCreateTable().Table<TestModelChildCompositePrimaryKey>()));

            Assert.DoesNotThrow(() => DataContext.Execute(cmd));
        }

        [Category("Drop")]
        [Test]
        public void TestDropTable()
        {
            var cmd = new SqlDropTable();
            cmd.Table("dbo.TestCreation");

            TestContext.Out.WriteLine(cmd.ToString());

            TestContext.Out.WriteLine(GetQueryParser().ToString(cmd));

            Assert.DoesNotThrow(() => DataContext.Execute(cmd));
        }

        [Category("Select")]
        [Test]
        public void TestSelectNoException()
        {
            Assert.DoesNotThrow(() => DataContext.Select<TestModelChild>().ToList());
        }

        [Category("Select")]
        [Test]
        public void TestSelectDynamicNoException()
        {
            Assert.DoesNotThrow(() => DataContext.Select(ModelMetadata<TestModelChild>.Instance).ToList());
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
            var query = DataContext.SelectFrom<TestModelChild>().Where(new SqlWhere().Name("Name").Eq(par));

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
        public void TestSelectDynamicWithParameter()
        {
            var par = new SqlParameter("par1");
            var query = DataContext.SelectFrom(ModelMetadata<TestModelChild>.Instance).Where(new SqlWhere().Name("Name").Eq(par));

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
            {
                result = (DateTime)DataContext.ExecuteScalar(new SqlSelect().Select(new SqlConstant(now)));
                //result = result.ToLocalTime();
            }
            else if (DataContext is MSSQL_DataContext)
                result = (DateTime)DataContext.ExecuteScalar(new SqlSelect().Select(new SqlComposition(new SqlCustom("cast ("), new SqlConstant(now), new SqlCustom(" as datetime)"))));
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive("Unsupported");
            else
                result = DateTime.Parse(DataContext.ExecuteScalar(new SqlSelect().Select(new SqlConstant(now))).ToString());

            //now = now.ToLocalTime();

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
            var result = DataContext.SelectFrom<TestModelChild>().Where(new SqlWhere().Name("FValue").LeValue(1.1F)).OrderBy("Name").Select();
            Assert.That(result.Count() == 1);
        }

        [Category("Select")]
        [Test]
        public void TestSelectLessOrEqualDynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModelChild>.Instance).Where(new SqlWhere().Name("FValue").LeValue(1.1F)).OrderBy("Name").Select();
            Assert.That(result.Count() == 1);
        }

        [Category("Select")]
        [Test]
        public void TestSelectWithRecursiveReference()
        {
            var result = DataContext.Select<TestModelExtra>().ToArray();
            Assert.That(result.Length == 2 && result[0].Extra == result[0] && result[1].Extra == result[1]);
        }

        [Category("Select")]
        [Test]
        public void TestSelectWithRecursiveReferenceDynamic()
        {
            var result = DataContext.Select(ModelMetadata<TestModelExtra>.Instance).ToArray();
            Assert.That(result.Length == 2 && result[0].Extra == result[0] && result[1].Extra == result[1]);
        }

        [Category("Select")]
        [Test]
        public void TestSelectMany()
        {
            var result = DataContext.SelectFrom<TestModel>().OrderBy("Name").Select().ToArray();

            var check1 = result.Length == 3;
            var check2 = result[0].Id == 1 && result[0].LongId == 1L && result[0].ShortId == 1 && result[0].Name == "TestModel1" && result[0].CharCode == "a" && result[0].Checked == false && result[0].Value == 1 && result[0].FValue == 1.1F && result[0].GValue == 1.2 && result[0].Money == (decimal)1.3 && result[0].Timestamp == DateTime.Parse("2024-01-01") && result[0].Duration == TimeSpan.Parse("23:59:59") && BitConverter.ToString(result[0].bindata) == "01-02-03-04";
            var check3 = result[1].Id == 2 && result[1].LongId == 2L && result[1].ShortId == 2 && result[1].Name == "TestModel2" && result[1].CharCode == "b" && result[1].Checked == true && result[1].Value == 1 && result[1].FValue == 1.1F && result[1].GValue == 1.2 && result[1].Money == (decimal)1.3 && result[1].Timestamp == DateTime.Parse("2024-03-01") && result[1].Duration == TimeSpan.Parse("01:01:01") && BitConverter.ToString(result[1].bindata) == "FF-FF-FF-FF";
            var check4 = result[2].Id == 3 && result[2].LongId == null && result[2].ShortId == null && result[2].Name == "TestModel3" && result[2].CharCode == "b" && result[2].Checked == true && result[2].Value == 1 && result[2].FValue == 1.1F && result[2].GValue == 1.3 && result[2].Money == (decimal)1.4 && result[2].Timestamp == DateTime.Parse("2024-04-01") && result[2].Duration == TimeSpan.Parse("01:01:01") && BitConverter.ToString(result[2].bindata) == "00-00-FF-00";

            TestContext.Out.WriteLine($"{check1} {check2} {check3} {check4}");

            Assert.That(
                (
                check1
                &&
                check2
                && check3
                && check4
                )
            );
        }

        [Category("Select")]
        [Test]
        public void TestSelectManyDynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModel>.Instance).OrderBy("Name").Select().ToArray();

            var check1 = result.Length == 3;
            var check2 = result[0].Id == 1 && result[0].LongId == 1L && result[0].ShortId == 1 && result[0].Name == "TestModel1" && result[0].CharCode == "a" && result[0].Checked == false && result[0].Value == 1 && result[0].FValue == 1.1F && result[0].GValue == 1.2 && result[0].Money == (decimal)1.3 && result[0].Timestamp == DateTime.Parse("2024-01-01") && result[0].Duration == TimeSpan.Parse("23:59:59") && BitConverter.ToString(result[0].bindata) == "01-02-03-04";
            var check3 = result[1].Id == 2 && result[1].LongId == 2L && result[1].ShortId == 2 && result[1].Name == "TestModel2" && result[1].CharCode == "b" && result[1].Checked == true && result[1].Value == 1 && result[1].FValue == 1.1F && result[1].GValue == 1.2 && result[1].Money == (decimal)1.3 && result[1].Timestamp == DateTime.Parse("2024-03-01") && result[1].Duration == TimeSpan.Parse("01:01:01") && BitConverter.ToString(result[1].bindata) == "FF-FF-FF-FF";
            var check4 = result[2].Id == 3 && result[2].LongId == null && result[2].ShortId == null && result[2].Name == "TestModel3" && result[2].CharCode == "b" && result[2].Checked == true && result[2].Value == 1 && result[2].FValue == 1.1F && result[2].GValue == 1.3 && result[2].Money == (decimal)1.4 && result[2].Timestamp == DateTime.Parse("2024-04-01") && result[2].Duration == TimeSpan.Parse("01:01:01") && BitConverter.ToString(result[2].bindata) == "00-00-FF-00";

            TestContext.Out.WriteLine($"{check1} {check2} {check3} {check4}");

            Assert.That(
                (
                check1
                &&
                check2
                && check3
                && check4
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
                result[0].Timestamp.Value == DateTime.Parse("2024-01-01") &&
                result[0].Parent.Id == 1 &&
                result[0].Extra.Id == 1
                )
                &&
               (
               result[1].Id == 2 &&
                result[1].Name == "TestModelChild2" &&
                result[1].FValue == 1.15F &&
                result[1].GValue == 1.25 &&
                result[1].Timestamp.Value == DateTime.Parse("2023-01-01") &&
                result[1].Parent.Id == 3 &&
                result[1].Extra.Id == 2
               )
            );
        }

        [Category("Select")]
        [Test]
        public void TestSelectMany1Dynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModelChild>.Instance).OrderBy("Name").Select().ToArray();
            Assert.That(
                (
                result.Length > 0 &&

                result[0].Id == 1 &&
                result[0].Name == "TestModelChild1" &&
                result[0].FValue == 1.1F &&
                result[0].GValue == 1.2 &&
                result[0].Timestamp == DateTime.Parse("2024-01-01") &&
                result[0].Parent.Id == 1 &&
                result[0].Extra.Id == 1
                )
                &&
               (
               result[1].Id == 2 &&
                result[1].Name == "TestModelChild2" &&
                result[1].FValue == 1.15F &&
                result[1].GValue == 1.25 &&
                result[1].Timestamp == DateTime.Parse("2023-01-01") &&
                result[1].Parent.Id == 3 &&
                result[1].Extra.Id == 2
               )
            );
        }

        [Category("Select")]
        [Test]
        public void TestSelectCompositePrimaryKey()
        {
            var result = DataContext.Select<TestModelParentCompositePrimaryKey>().ToArray();

            Assert.That(result.Length == 4);
        }

        [Category("Select")]
        [Test]
        public void TestSelectCompositePrimaryKeyDynamic()
        {
            var result = DataContext.Select(ModelMetadata<TestModelParentCompositePrimaryKey>.Instance).ToArray();

            Assert.That(result.Length == 4);
        }

        [Category("Insert")]
        [Test]
        public void TestInsertPrimaryKeyAsForeignKey()
        {
            var c = new TestModelChildCompositePrimaryKey()
            {
                i = 5,
                j = 1,
                k = "stu"
            };
            DataContext.Insert(c);
            var n = new TestModelPrimaryKeyAsForeignKey()
            {
                Child = c,
                k = "stu"
            };

            DataContext.Insert(n);

            var result = DataContext.SelectFrom<TestModelPrimaryKeyAsForeignKey>().OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 7;
            bool check2 = result[0].Child.i == 1 && result[0].Child.j == 1 && result[0].k == "abc";
            bool check4 = result[1].Child.i == 1 && result[1].Child.j == 2 && result[1].k == "ghi";
            bool check5 = result[2].Child.i == 2 && result[2].Child.j == 2 && result[2].k == "def";
            bool check6 = result[3].Child.i == 2 && result[3].Child.j == 3 && result[3].k == "jkl";
            bool check7 = result[4].Child.i == 3 && result[4].Child.j == 3 && result[4].k == "mno";
            bool check8 = result[5].Child.i == 4 && result[5].Child.j == 1 && result[5].k == "pqr";
            bool check3 = result[6].Child.i == 5 && result[6].Child.j == 1 && result[6].k == "stu";

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7 && check8);
        }

        [Category("Insert")]
        [Test]
        public void TestInsertPrimaryKeyAsForeignKeyDynamic()
        {
            dynamic c = new DynamicModel();
            c.i = 5;
            c.j = 1;
            c.k = "stu";
            DataContext.Insert(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance, c);

            dynamic n = new DynamicModel();

            n.Child = c;
            n.k = "stu";

            DataContext.Insert(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance, n);

            var result = DataContext.SelectFrom(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 7;
            bool check2 = result[0].Child.i == 1 && result[0].Child.j == 1 && result[0].k == "abc";
            bool check4 = result[1].Child.i == 1 && result[1].Child.j == 2 && result[1].k == "ghi";
            bool check5 = result[2].Child.i == 2 && result[2].Child.j == 2 && result[2].k == "def";
            bool check6 = result[3].Child.i == 2 && result[3].Child.j == 3 && result[3].k == "jkl";
            bool check7 = result[4].Child.i == 3 && result[4].Child.j == 3 && result[4].k == "mno";
            bool check8 = result[5].Child.i == 4 && result[5].Child.j == 1 && result[5].k == "pqr";
            bool check3 = result[6].Child.i == 5 && result[6].Child.j == 1 && result[6].k == "stu";

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7 && check8);
        }

        [Category("Select")]
        [Test]
        public void TestSelectPrimaryKeyAsForeignKey()
        {
            var result = DataContext.SelectFrom<TestModelPrimaryKeyAsForeignKey>().OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 6;
            bool check2 = result[0].Child.i == 1 && result[0].Child.j == 1 && result[0].k == "abc";
            bool check4 = result[1].Child.i == 1 && result[1].Child.j == 2 && result[1].k == "ghi";
            bool check3 = result[2].Child.i == 2 && result[2].Child.j == 2 && result[2].k == "def";
            bool check5 = result[3].Child.i == 2 && result[3].Child.j == 3 && result[3].k == "jkl";
            bool check6 = result[4].Child.i == 3 && result[4].Child.j == 3 && result[4].k == "mno";
            bool check7 = result[5].Child.i == 4 && result[5].Child.j == 1 && result[5].k == "pqr";

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Select")]
        [Test]
        public void TestSelectPrimaryKeyAsForeignKeyDynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 6;
            bool check2 = result[0].Child.i == 1 && result[0].Child.j == 1 && result[0].k == "abc";
            bool check4 = result[1].Child.i == 1 && result[1].Child.j == 2 && result[1].k == "ghi";
            bool check3 = result[2].Child.i == 2 && result[2].Child.j == 2 && result[2].k == "def";
            bool check5 = result[3].Child.i == 2 && result[3].Child.j == 3 && result[3].k == "jkl";
            bool check6 = result[4].Child.i == 3 && result[4].Child.j == 3 && result[4].k == "mno";
            bool check7 = result[5].Child.i == 4 && result[5].Child.j == 1 && result[5].k == "pqr";

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Update")]
        [Test]
        public void TestUpdatePrimaryKey()
        {
            var model = DataContext.SelectFrom<TestModelCompositePrimaryKey>().Where(m => m.i == 1 && m.j == 1).Select().ToArray()[0];
            var model1 = new TestModelCompositePrimaryKey();
            ModelMapper<TestModelCompositePrimaryKey>.CopyValues(model, model1);
            model1.i = 500;
            var sql = new SqlUpdate().From<TestModelCompositePrimaryKey>().Where(model).Value(model1);
            DataContext.Execute(sql);

            var result = DataContext.SelectFrom<TestModelCompositePrimaryKey>().OrderBy("i", "j").Select().ToArray();
            Assert.That(
                result.Length == 6 &&
                result[0].i == 1 && result[0].j == 2 && result[0].k == "ghi" &&
                result[1].i == 2 && result[1].j == 2 && result[1].k == "def" &&
                result[2].i == 2 && result[2].j == 3 && result[2].k == "jkl" &&
                result[3].i == 3 && result[3].j == 3 && result[3].k == "mno" &&
                result[4].i == 4 && result[4].j == 1 && result[4].k == "pqr" &&
                result[5].i == 500 && result[5].j == 1 && result[5].k == "abc"
                );
        }

        [Category("Update")]
        [Test]
        public void TestUpdatePrimaryKeyDynamic()
        {
            dynamic m = DataContext.SelectFrom(ModelMetadata<TestModelCompositePrimaryKey>.Instance).Where(new SqlWhere().Name("i").EqValue(1).AndName("j").EqValue(1)).Select().ToArray()[0];
            dynamic m1 = new DynamicModel();
            DynamicMapper.CopyValues(ModelMetadata<TestModelCompositePrimaryKey>.Instance, m, m1);
            m1.i = 500;
            var sql = new SqlUpdate().From(ModelMetadata<TestModelCompositePrimaryKey>.Instance);
            SqlUpdateExtensions.WhereDynamic(sql, ModelMetadata<TestModelCompositePrimaryKey>.Instance, m);
            SqlUpdateExtensions.ValueDynamic(sql, ModelMetadata<TestModelCompositePrimaryKey>.Instance, m1);
            DataContext.Execute(sql);

            var result = DataContext.SelectFrom(ModelMetadata<TestModelCompositePrimaryKey>.Instance).OrderBy("i", "j").Select().ToArray();
            Assert.That(
                result.Length == 6 &&
                result[0].i == 1 && result[0].j == 2 && result[0].k == "ghi" &&
                result[1].i == 2 && result[1].j == 2 && result[1].k == "def" &&
                result[2].i == 2 && result[2].j == 3 && result[2].k == "jkl" &&
                result[3].i == 3 && result[3].j == 3 && result[3].k == "mno" &&
                result[4].i == 4 && result[4].j == 1 && result[4].k == "pqr" &&
                result[5].i == 500 && result[5].j == 1 && result[5].k == "abc"
                );
        }

        [Category("Update")]
        [Test]
        public void TestUpdatePrimaryKeyAsForeignKey()
        {
            var result = DataContext.SelectFrom<TestModelPrimaryKeyAsForeignKey>().OrderBy("Child_i", "Child_j").Select().ToArray();

            var result1 = DataContext.SelectFrom<TestModelChildCompositePrimaryKey>().OrderBy("i", "j").Select().ToArray();


            var a = result[0];
            var b = result1[6];

            var old_b = a.Child;

            a.Child = b;

            DataContext.Execute(new SqlUpdate().From<TestModelPrimaryKeyAsForeignKey>().Value(a.Child.i, a.Child.j, a.k).Where(new SqlWhere().Name("Child_i").EqValue(old_b.i).AndName("Child_j").EqValue(old_b.j)));

            result = DataContext.SelectFrom<TestModelPrimaryKeyAsForeignKey>().OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 6;
            bool check4 = result[0].Child.i == 1 && result[0].Child.j == 2 && result[0].k == "ghi";
            bool check3 = result[1].Child.i == 2 && result[1].Child.j == 2 && result[1].k == "def";
            bool check5 = result[2].Child.i == 2 && result[2].Child.j == 3 && result[2].k == "jkl";
            bool check6 = result[3].Child.i == 3 && result[3].Child.j == 3 && result[3].k == "mno";
            bool check7 = result[4].Child.i == 4 && result[4].Child.j == 1 && result[4].k == "pqr";
            bool check2 = result[5].Child.i == 5 && result[5].Child.j == 5 && result[5].k == "abc";

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Update")]
        [Test]
        public void TestUpdatePrimaryKeyAsForeignKeyDynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).OrderBy("Child_i", "Child_j").Select().ToArray();

            var result1 = DataContext.SelectFrom(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance).OrderBy("i", "j").Select().ToArray();


            var a = result[0];
            var b = result1[6];

            var old_b = a.Child;

            a.Child = b;

            // так не получися, т.к. меняется поле первичного ключа
            //DataContext.Update(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance, a);

            DataContext.Execute(new SqlUpdate().From(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).Value((object)a.Child.i, (object)a.Child.j, (object)a.k).Where(new SqlWhere().Name("Child_i").EqValue((object)(old_b.i)).AndName("Child_j").EqValue((object)(old_b.j))));

            result = DataContext.SelectFrom(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 6;
            bool check4 = result[0].Child.i == 1 && result[0].Child.j == 2 && result[0].k == "ghi";
            bool check3 = result[1].Child.i == 2 && result[1].Child.j == 2 && result[1].k == "def";
            bool check5 = result[2].Child.i == 2 && result[2].Child.j == 3 && result[2].k == "jkl";
            bool check6 = result[3].Child.i == 3 && result[3].Child.j == 3 && result[3].k == "mno";
            bool check7 = result[4].Child.i == 4 && result[4].Child.j == 1 && result[4].k == "pqr";
            bool check2 = result[5].Child.i == 5 && result[5].Child.j == 5 && result[5].k == "abc";

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Delete")]
        [Test]
        public void TestDeletePrimaryKeyAsForeignKey()
        {
            var result = DataContext.SelectFrom<TestModelPrimaryKeyAsForeignKey>().OrderBy("Child_i", "Child_j").Select().ToArray();

            var a = result[0];

            DataContext.Delete(a);

            result = DataContext.SelectFrom<TestModelPrimaryKeyAsForeignKey>().OrderBy("Child_i", "Child_j").Select().ToArray();

            bool check1 = result.Length == 5;
            bool check4 = result[0].Child.i == 1 && result[0].Child.j == 2 && result[0].k == "ghi";
            bool check3 = result[1].Child.i == 2 && result[1].Child.j == 2 && result[1].k == "def";
            bool check5 = result[2].Child.i == 2 && result[2].Child.j == 3 && result[2].k == "jkl";
            bool check6 = result[3].Child.i == 3 && result[3].Child.j == 3 && result[3].k == "mno";
            bool check7 = result[4].Child.i == 4 && result[4].Child.j == 1 && result[4].k == "pqr";

            Assert.That(check1 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Delete")]
        [Test]
        public void TestDeletePrimaryKeyAsForeignKeyDynamic()
        {
            try
            {
                var result = DataContext.SelectFrom(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).OrderBy("Child_i", "Child_j").Select().ToArray();

                var a = result[0];

                DataContext.Delete(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance, a);

                result = DataContext.SelectFrom(ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance).OrderBy("Child_i", "Child_j").Select().ToArray();

                bool check1 = result.Length == 5;
                bool check4 = result[0].Child.i == 1 && result[0].Child.j == 2 && result[0].k == "ghi";
                bool check3 = result[1].Child.i == 2 && result[1].Child.j == 2 && result[1].k == "def";
                bool check5 = result[2].Child.i == 2 && result[2].Child.j == 3 && result[2].k == "jkl";
                bool check6 = result[3].Child.i == 3 && result[3].Child.j == 3 && result[3].k == "mno";
                bool check7 = result[4].Child.i == 4 && result[4].Child.j == 1 && result[4].k == "pqr";

                Assert.That(check1 && check3 && check4 && check5 && check6 && check7);
            }
            catch
            {
                throw;
            }
        }

        [Category("Select")]
        [Test]
        public void TestSelectCompositeForeignKey()
        {
            //var result1 = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChildCompositePrimaryKey>().OrderBy("i", "j")).ToArray();

            var result = DataContext.Select<TestModelChildCompositePrimaryKey>(new SqlSelect().From<TestModelChildCompositePrimaryKey>().OrderBy("i", "j")).ToArray();

            bool check1 = result.Length == 7;
            bool check2 = result[0].i == 1 && result[0].j == 1 && result[0].Parent.i == 1 && result[0].Parent.j == 1;
            bool check3 = result[1].i == 1 && result[1].j == 2 && result[1].Parent.i == 1 && result[1].Parent.j == 2;
            bool check4 = result[2].i == 2 && result[2].j == 2 && result[2].Parent.i == 1 && result[2].Parent.j == 1;
            bool check5 = result[3].i == 2 && result[3].j == 3 && result[3].Parent.i == 2 && result[3].Parent.j == 2;
            bool check6 = result[4].i == 3 && result[4].j == 3 && result[4].Parent.i == 3 && result[4].Parent.j == 3;
            bool check7 = result[5].i == 4 && result[5].j == 1 && result[5].Parent == null;
            bool check8 = result[6].i == 5 && result[6].j == 5 && result[6].Parent.i == 1 && result[6].Parent.j == 2;

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Select")]
        [Test]
        public void TestSelectCompositeForeignKeyDynamic()
        {
            //var result1 = DataContext.ExecuteWithResult(new SqlSelect().From(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance).OrderBy("i", "j")).ToArray();

            var result = DataContext.Select(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance, new SqlSelect().From(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance).OrderBy("i", "j")).ToArray();

            bool check1 = result.Length == 7;
            bool check2 = result[0].i == 1 && result[0].j == 1 && result[0].Parent.i == 1 && result[0].Parent.j == 1;
            bool check3 = result[1].i == 1 && result[1].j == 2 && result[1].Parent.i == 1 && result[1].Parent.j == 2;
            bool check4 = result[2].i == 2 && result[2].j == 2 && result[2].Parent.i == 1 && result[2].Parent.j == 1;
            bool check5 = result[3].i == 2 && result[3].j == 3 && result[3].Parent.i == 2 && result[3].Parent.j == 2;
            bool check6 = result[4].i == 3 && result[4].j == 3 && result[4].Parent.i == 3 && result[4].Parent.j == 3;
            bool check7 = result[5].i == 4 && result[5].j == 1 && result[5].Parent == null;
            bool check8 = result[6].i == 5 && result[6].j == 5 && result[6].Parent.i == 1 && result[6].Parent.j == 2;

            Assert.That(check1 && check2 && check3 && check4 && check5 && check6 && check7);
        }

        [Category("Update")]
        [Test]
        public void TestUpdateCompositeForeignKey()
        {
            var child = DataContext.SelectFrom<TestModelChildCompositePrimaryKey>().Where(new SqlWhere().Name("Parent_i").IsNull().AndName("Parent_j").IsNull()).Select().ToArray();

            bool check1 = child.Length == 1;

            bool check11 = child[0].i == 4 && child[0].j == 1;

            var parent = DataContext.SelectFrom<TestModelParentCompositePrimaryKey>().Where(new SqlWhere().Name("i").EqValue(3).AndName("j").EqValue(3)).Select().ToArray();

            bool check2 = parent.Length == 1;

            bool check3 = child[0].Parent == null;

            child[0].Parent = parent[0];

            DataContext.Update(child[0]);

            var result = DataContext.SelectFrom<TestModelChildCompositePrimaryKey>().OrderBy("i", "j").Select().ToArray();

            bool check4 = result.Length == 7;
            bool check5 = result[0].i == 1 && result[0].j == 1 && result[0].Parent.i == 1 && result[0].Parent.j == 1;
            bool check6 = result[1].i == 1 && result[1].j == 2 && result[1].Parent.i == 1 && result[1].Parent.j == 2;
            bool check7 = result[2].i == 2 && result[2].j == 2 && result[2].Parent.i == 1 && result[2].Parent.j == 1;
            bool check8 = result[3].i == 2 && result[3].j == 3 && result[3].Parent.i == 2 && result[3].Parent.j == 2;
            bool check9 = result[4].i == 3 && result[4].j == 3 && result[4].Parent.i == 3 && result[4].Parent.j == 3;
            bool check10 = result[5].i == 4 && result[5].j == 1 && result[5].Parent != null && result[5].Parent.i == 3 && result[5].Parent.j == 3;
            bool check12 = result[6].i == 5 && result[6].j == 5 && result[6].Parent.i == 1 && result[6].Parent.j == 2;

            Assert.That(check1 && check11 && check2 && check3 && check4 && check5 && check5 && check6 && check6 && check7 && check8 && check9 && check10 && check12);
        }

        [Category("Update")]
        [Test]
        public void TestUpdateCompositeForeignKeyDynamic()
        {
            var child = DataContext.SelectFrom(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance).Where(new SqlWhere().Name("Parent_i").IsNull().AndName("Parent_j").IsNull()).Select().ToArray();

            bool check1 = child.Length == 1;

            bool check11 = child[0].i == 4 && child[0].j == 1;

            var parent = DataContext.SelectFrom(ModelMetadata<TestModelParentCompositePrimaryKey>.Instance).Where(new SqlWhere().Name("i").EqValue(3).AndName("j").EqValue(3)).Select().ToArray();

            bool check2 = parent.Length == 1;

            bool check3 = child[0].Parent == null;

            child[0].Parent = parent[0];

            DataContext.Update(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance, child[0]);

            var result = DataContext.SelectFrom(ModelMetadata<TestModelChildCompositePrimaryKey>.Instance).OrderBy("i", "j").Select().ToArray();

            bool check4 = result.Length == 7;
            bool check5 = result[0].i == 1 && result[0].j == 1 && result[0].Parent.i == 1 && result[0].Parent.j == 1;
            bool check6 = result[1].i == 1 && result[1].j == 2 && result[1].Parent.i == 1 && result[1].Parent.j == 2;
            bool check7 = result[2].i == 2 && result[2].j == 2 && result[2].Parent.i == 1 && result[2].Parent.j == 1;
            bool check8 = result[3].i == 2 && result[3].j == 3 && result[3].Parent.i == 2 && result[3].Parent.j == 2;
            bool check9 = result[4].i == 3 && result[4].j == 3 && result[4].Parent.i == 3 && result[4].Parent.j == 3;
            bool check10 = result[5].i == 4 && result[5].j == 1 && result[5].Parent != null && result[5].Parent.i == 3 && result[5].Parent.j == 3;
            bool check12 = result[6].i == 5 && result[6].j == 5 && result[6].Parent.i == 1 && result[6].Parent.j == 2;

            Assert.That(check1 && check11 && check2 && check3 && check4 && check5 && check5 && check6 && check6 && check7 && check8 && check9 && check10 && check12);
        }

        [Category("Delete")]
        [Test]
        public void TestDeleteCompositePrimaryKey()
        {
            var child = DataContext.SelectFrom<TestModelCompositePrimaryKey>().Where(new SqlWhere().Name("i").EqValue(1).AndName("i").EqValue(1)).Select().ToArray();

            DataContext.Delete(child[0]);

            var result = DataContext.Select<TestModelCompositePrimaryKey>(new SqlSelect().From<TestModelCompositePrimaryKey>().OrderBy("i", "j")).ToArray();

            bool check1 = result.Length == 5;
            bool check2 = result[0].i == 1 && result[0].j == 2;
            bool check3 = result[1].i == 2 && result[1].j == 2;
            bool check4 = result[2].i == 2 && result[2].j == 3;
            bool check5 = result[3].i == 3 && result[3].j == 3;
            bool check7 = result[4].i == 4 && result[4].j == 1;
        }

        [Category("Delete")]
        [Test]
        public void TestDeleteCompositePrimaryKeyDynamic()
        {
            var child = DataContext.SelectFrom(ModelMetadata<TestModelCompositePrimaryKey>.Instance).Where(new SqlWhere().Name("i").EqValue(1).AndName("i").EqValue(1)).Select().ToArray();

            DataContext.Delete(ModelMetadata<TestModelCompositePrimaryKey>.Instance, child[0]);

            var result = DataContext.Select(ModelMetadata<TestModelCompositePrimaryKey>.Instance, new SqlSelect().From(ModelMetadata<TestModelCompositePrimaryKey>.Instance).OrderBy("i", "j")).ToArray();

            bool check1 = result.Length == 5;
            bool check2 = result[0].i == 1 && result[0].j == 2;
            bool check3 = result[1].i == 2 && result[1].j == 2;
            bool check4 = result[2].i == 2 && result[2].j == 3;
            bool check5 = result[3].i == 3 && result[3].j == 3;
            bool check7 = result[4].i == 4 && result[4].j == 1;
        }

        [Category("Select")]
        [TestCase(1, ExpectedResult = 1)]
        [TestCase(0, ExpectedResult = 0)]
        public int TestWhere(int id)
        {
            var ar = DataContext.SelectFrom<TestModelChild>().Where("Id", id).Select().ToArray();
            return ar.Length;
        }

        [Test]
        public void TestWhereExpression()
        {
            var cmd = DataContext.SelectFrom<TestModelChild>().Where(m => m.Id == 1);
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 1 && result[0].Id == 1);
        }

        [Test]
        public void TestWhereExpression1()
        {
            var cmd = DataContext.SelectFrom<TestModelSimple>().Where(m => m.Id >= 2);
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 2);
        }

        [Test]
        public void TestWhereExpression2()
        {
            var cmd = DataContext.SelectFrom<TestModelSimple>().Where(m => m.Id == null);
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 1 && result[0].Id == null);
        }

        [Test]
        public void TestWhereExpression3()
        {
            int a = 1;
            var cmd = DataContext.SelectFrom<TestModelChild>().Where(m => m.Id == a);
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 1 && result[0].Id == 1);
        }

        [Test]
        public void TestWhereExpression4()
        {
            var a = new
            {
                i = 1
            };
            var cmd = DataContext.SelectFrom<TestModelChild>().Where(m => m.Id == a.i);
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 1 && result[0].Id == 1);
        }

        class TestWhereExpression5Test
        {
            public int a = 1;
        }

        [Test]
        public void TestWhereExpression5()
        {
            var a = new TestWhereExpression5Test();
            var cmd = DataContext.SelectFrom<TestModelChild>().Where(m => m.Id == a.a);
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 1 && result[0].Id == 1);
        }

        [Test]
        public void TestWhereExpression6()
        {
            var f = new Func<int>( () => 1);
            var cmd = DataContext.SelectFrom<TestModelChild>().Where(m => m.Id == f());
            TestContext.Out.WriteLine(GetQueryParser().SimplifyQuery(cmd.Query));
            var result = cmd.Select().ToArray();
            Assert.That(result.Length == 1 && result[0].Id == 1);
        }

        [Category("Select")]
        [Test]
        public void TestWhereIsNull()
        {
            var ar = DataContext.SelectFrom<TestModelSimple>().Where("Id", null).Select().ToArray();
            TestContext.Out.WriteLine($"{ar.Length} {string.Join<TestModelSimple>(",", ar)}"); ;
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
                FValue = float.MaxValue / 2,
                GValue = double.MaxValue / 2,
                Parent = null,
                Timestamp = DateTime.MinValue,
                Value = int.MinValue
            };
            DataContext.Insert(m);

            var newCount = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChild>()).Count();

            if (isLong)
                Assert.That((long)count + 1 == newCount);
            else
                Assert.That(count + 1 == newCount);
        }

        public void TestInsertInMemory()
        {
            var count = DataContext.ExecuteWithResult(new SqlSelect().From<TestModelChild>()).Count();

            var m = new TestModelChild()
            {
                Id = 1,
                Name = $"{nameof(TestModelChild)}",
                Extra = null,
                FValue = float.MaxValue / 2,
                GValue = double.MaxValue / 2,
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
                BitConverter.ToString(inserted.bindata) == BitConverter.ToString(m.bindata)
                )

                );
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
                Value = int.MinValue / 2,
                FValue = float.MaxValue / 2,
                GValue = double.MinValue / 2,
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
                inserted.Timestamp.ToLocalTime() == m.Timestamp.ToLocalTime() &&
                inserted.Duration == m.Duration &&
                inserted.Guid == m.Guid &&
                BitConverter.ToString(inserted.bindata) == BitConverter.ToString(m.bindata)
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

            var model = r.ToArray()[0];
            model.Name = "NewName";

            DataContext.Update(model);

            var n = DataContext.SelectFrom<TestModelChild>().Where("Id", id).Select().ToArray()[0];
            var nn = DataContext.SelectFrom<TestModelChild>().Where(new SqlWhere().Name("Id").NeValue(id)).Select().ToArray();

            Assert.That(n.Name == "NewName" && nn.All(m => m.Name != "NewName"));
        }

        [Category("Update")]
        [TestCase(arg: 1)]
        [TestCase(arg: 0)]
        public void TestUpdateDynamic(int id)
        {
            var r = DataContext.SelectFrom(ModelMetadata<TestModelChild>.Instance).Where("Id", id).Select().ToArray();

            if (r.Count() == 1)
                Assert.That(true);
            else
                Assert.Pass("no model found");

            var model = r.ToArray()[0];
            model.Name = "NewName";

            DataContext.Update(ModelMetadata<TestModelChild>.Instance, model);

            var n = DataContext.SelectFrom(ModelMetadata<TestModelChild>.Instance).Where("Id", id).Select().ToArray()[0];
            var nn = DataContext.SelectFrom(ModelMetadata<TestModelChild>.Instance).Where(new SqlWhere().Name("Id").NeValue(id)).Select().ToArray();

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
            var s = BitConverter.ToString(new byte[] { 255, 255, 0, 255 });

            result[0].bindata = new byte[] { 255, 255, 0, 255 };

            DataContext.Update(result[0]);

            result = DataContext.Select<TestModel>(new SqlSelect().From<TestModel>().Where("Id", 1)).ToArray();

            Assert.That(s == BitConverter.ToString(result[0].bindata));
        }

        [Category("Update")]
        [Test]
        public void TestUpdateBinaryDynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModel>.Instance).Where("Id", 1).Select().ToArray();
            TestContext.Out.WriteLine(JsonConvert.SerializeObject(result));
            var s = BitConverter.ToString(new byte[] { 255, 255, 0, 255 });

            result[0].bindata = new byte[] { 255, 255, 0, 255 };

            DataContext.Update(ModelMetadata<TestModel>.Instance, result[0]);

            result = DataContext.Select(ModelMetadata<TestModel>.Instance, new SqlSelect().From(ModelMetadata<TestModel>.Instance).Where("Id", 1)).ToArray();

            Assert.That(s == BitConverter.ToString(result[0].bindata));
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

            if (context is SQLite_DataContext)
                Assert.Pass($"{nameof(SQLite_DataContext)} does not support procedures");

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
                    .Where(new SqlWhere().AndName("a").EqValue(1).AndName("b").EqValue(2).OrName("c").EqValue(3))
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
              .Value(new SqlSelect().From(new SqlName("test1")).Select(new SqlName("a")).Where(new SqlWhere(new SqlName("b")).Eq(new SqlConstant(1))), new SqlCustom("select count() from sys.tables"), new SqlConstant(null));



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
                    new SqlWhere()
                    .And(new SqlName("a"))
                    .EqValue(5)
                    .And(
                        new SqlWhere()
                        .AndName("b")
                        .EqValue("ds")
                        .OrName("c")
                        .EqValue(Guid.NewGuid())
                    )
                );
            WriteSQLQuery(q);


            q = new SqlUpdate().From("test").Set("a").Value(1).Where(new SqlWhere().And(new SqlWhere().AndName("a").EqValue(5)).Or(new SqlWhere().AndName("a").LtValue(1)));

            WriteSQLQuery(q);
        }

        [Category("Delete")]
        [Test]
        public void TestDeleteCreating()
        {
            var q = new SqlDelete()
                .From("test")
                .Where(new SqlWhere().AndName("a").EqValue(1).AndName("b").EqValue(2).OrName("c").EqValue(3));
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
        public void TestDeleteAllFromTableDynamic()
        {
            var q = new SqlDelete().From(ModelMetadata<TestModelChild>.Instance);
            DataContext.Execute(q);

            var ar = DataContext.Select(ModelMetadata<TestModelChild>.Instance).ToArray();

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

        [Category("Delete")]
        [Test]
        public void TestDeleteSingleRowFromTableDynamic()
        {
            var q = new SqlDelete().From(ModelMetadata<TestModelChild>.Instance).Where(nameof(TestModelChild.Name), "TestModelChild1");
            DataContext.Execute(q);
            var l1 = DataContext.Select(ModelMetadata<TestModelChild>.Instance).ToArray().Length;

            DataContext.Delete(ModelMetadata<TestModelChild>.Instance, DataContext.Select(ModelMetadata<TestModelChild>.Instance).ToArray()[0]);

            var l2 = DataContext.Select(ModelMetadata<TestModelChild>.Instance).ToArray().Length;

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
            if (DataContext is SQLite_DataContext)
                Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support stored table functions");
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support stored table functions");

            var result = DataContext.CallTableFunction<TestModel>("dbo.testFunc");

            Assert.That(result.Count() == 3);
        }

        [Category("Function")]
        [Test]
        public void TestCallTableFunctionWithoutParamDynamic()
        {
            if (DataContext is SQLite_DataContext)
                Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support stored table functions");
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support stored table functions");

            var result = DataContext.CallTableFunction(ModelMetadata<TestModel>.Instance, "dbo.testFunc");

            Assert.That(result.Count() == 3);
        }

        [Category("Function")]
        [TestCase(arguments: 0)]
        [TestCase(arguments: 1)]
        public void TestCallTableFunctionWithParam(int i)
        {
            if (DataContext is SQLite_DataContext)
                Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support stored table functions");
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support stored table functions");

            var result = DataContext.CallTableFunction<TestModel>("dbo.testFuncParam", i);

            Assert.That(i == result.Count());
        }

        [Category("Function")]
        [TestCase(arguments: 0)]
        [TestCase(arguments: 1)]
        public void TestCallTableFunctionWithParamDynamic(int i)
        {
            if (DataContext is SQLite_DataContext)
                Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support stored table functions");
            else if (DataContext is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support stored table functions");

            var result = DataContext.CallTableFunction(ModelMetadata<TestModel>.Instance, "dbo.testFuncParam", i);

            Assert.That(i == result.Count());
        }

        [Category("Function")]
        [Test]
        public void TestCallScalarFunction()
        {
            if (DataContext is InMemory_SQLite_DataContext) Assert.Inconclusive("Unsupported");

            string fName = null;

            if (DataContext is SQLite_DataContext)
                fName = "datetime";
            if (DataContext is MSSQL_DataContext)
                fName = "getdate";
            if (DataContext is PostgreSQL_DataContext)
                fName = "now";

            if (fName == null) Assert.Fail();

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

            if (DataContext is SQLite_DataContext)
                fName = "length";
            if (DataContext is MSSQL_DataContext)
                fName = "len";
            if (DataContext is PostgreSQL_DataContext)
                fName = "length";

            if (fName == null)
                Assert.Fail();

            var result = DataContext.CallScalarFunction(fName, "abc");
            if (result is string resultStr)
                Assert.That(int.TryParse(resultStr, out int resultInt) && resultInt == 3);
            else if (result is int)
                Assert.That(((int)result) == 3);
            else if (result is long)
                Assert.That(((long)result) == 3);
        }

        [NoUniqueAttribute]
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

        [Category("Procedure")]
        [TestCase(arguments: 0)]
        [TestCase(arguments: 1)]
        public void TestCallProcedureWithParamAndReturnsDynamic(int i)
        {
            var context = DataManager.GetContext(alias);

            if (context is PostgreSQL_DataContext)
                Assert.Inconclusive($"{nameof(PostgreSQL_DataContext)} does not support procedures with result set.");
            if (context is SQLite_DataContext)
                Assert.Inconclusive($"{nameof(SQLite_DataContext)} does not support procedures with result set.");
            if (context is InMemory_SQLite_DataContext)
                Assert.Inconclusive($"{nameof(InMemory_SQLite_DataContext)} does not support procedures with result set.");

            var result = context.CallProcedure(ModelMetadata<testProcReturn_Result>.Instance, "dbo.testProcReturn", i).ToArray();
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

        [Category("Insert")]
        [Test]
        public void TestInsertGUIDDynamic()
        {

            dynamic a = new DynamicModel();
            var g = Guid.NewGuid();
            a.Id = g;
            a.Name = "child3";
            DataContext.Insert(ModelMetadata<TestModelGuidChild>.Instance, a);

            var r = DataContext.Select(ModelMetadata<TestModelGuidChild>.Instance, new SqlSelect().From(ModelMetadata<TestModelGuidChild>.Instance).Where("Id", g)).ToArray();

            Assert.That(r.Length == 1 && r[0].Id == g);
        }
    }
}

