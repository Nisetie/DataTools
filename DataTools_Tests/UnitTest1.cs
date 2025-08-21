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
using Newtonsoft.Json.Linq;
using NUnit.Framework.Internal;
using System.Data.SQLite;
using System.Net.Sockets;
using System.Security;

namespace DataTools_Tests
{
    public class TestData
    {
        public TestModel[] testModels;
        public TestModelExtra[] testModelExtras;
        public TestModelChild[] testModelChilds;
        public TestModelGuidParent[] testModelGuidParents;
        public TestModelGuidChild[] testModelGuidChilds;
        public TestModelSimple[] testModelSimples;
        public TestModelParentCompositePrimaryKey[] testModelParentCompositePrimaryKeys;
        public TestModelChildCompositePrimaryKey[] testModelChildCompositePrimaryKeys;
        public TestModelPrimaryKeyAsForeignKey[] testModelPrimaryKeyAsForeignKeys;
        public TestModelCompositePrimaryKey[] testModelCompositePrimaryKeys;

        public IEnumerable<IModelMetadata> Metadatas = [
            ModelMetadata<TestModel>.Instance,
            ModelMetadata<TestModelExtra>.Instance,
            ModelMetadata<TestModelChild>.Instance,
            ModelMetadata<TestModelGuidParent>.Instance,
            ModelMetadata<TestModelGuidChild>.Instance,
            ModelMetadata<TestModelSimple>.Instance,
            ModelMetadata<TestModelParentCompositePrimaryKey>.Instance,
            ModelMetadata<TestModelChildCompositePrimaryKey>.Instance,
            ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance,
            ModelMetadata<TestModelCompositePrimaryKey>.Instance
            ];

        public TestData()
        {
            testModels = new TestModel[] {
                new TestModel() {
                    Id =1,
                    LongId =1L,
                    ShortId = 1,
                    Name = "TestModel1",
                    CharCode ="a",
                    Checked = false,
                    Value = 1,
                    FValue =1.1F,
                    GValue = 1.2,
                    Money = (decimal)1.3,
                    Timestamp = DateTime.Parse("2024-01-01"),
                    Duration = TimeSpan.Parse("23:59:59"),
                    Guid = Guid.NewGuid(),
                    bindata = Convert.FromHexString("01020304")
                },
                new TestModel() {
                    Id =2,
                    LongId =2L,
                    ShortId = 2,
                    Name = "TestModel2",
                    CharCode ="b",
                    Checked = true,
                    Value = 1,
                    FValue =1.1F,
                    GValue = 1.2,
                    Money = (decimal)1.3,
                    Timestamp = DateTime.Parse("2024-03-01"),
                    Duration = TimeSpan.Parse("01:01:01"),
                    Guid = Guid.NewGuid(),
                    bindata = Convert.FromHexString("FFFFFFFF")
                },
                new TestModel() {
                    Id =3,
                    LongId =null,
                    ShortId = null,
                    Name = "TestModel3",
                    CharCode ="b",
                    Checked = true,
                    Value = 1,
                    FValue =1.1F,
                    GValue = 1.3,
                    Money = (decimal)1.4,
                    Timestamp = DateTime.Parse("2024-04-01"),
                    Duration = TimeSpan.Parse("01:01:01"),
                    Guid = Guid.NewGuid(),
                    bindata = Convert.FromHexString("0000FF00")
                }
            };

            testModelExtras = new TestModelExtra[] {
                new TestModelExtra() {
                    Id = 1,
                    Name = "TestModelExtra1",
                    Value = 1,
                    FValue = 1.1F,
                    GValue = 1.2,
                    Timestamp = DateTime.Parse("2024-01-01"),
                    Extra = null
                },
                new TestModelExtra() {
                    Id = 2,
                    Name = "TestModelExtra2",
                    Value = 1,
                    FValue = 1.1F,
                    GValue = 1.2,
                    Timestamp = DateTime.Parse("2023-01-01"),
                    Extra = null
                }
            };
            testModelExtras[0].Extra = testModelExtras[0];
            testModelExtras[1].Extra = testModelExtras[1];

            testModelChilds = new TestModelChild[]
            {
                new TestModelChild()
                {
                    Id = 1,
                    Name = "TestModelChild1",
                    Value = 1,
                    FValue = 1.1f,
                    GValue = 1.2,
                    Timestamp= DateTime.Parse("2024-01-01"),
                    Parent = testModels[0],
                    Extra = testModelExtras[0]
                },
                new TestModelChild()
                {
                    Id = 2,
                    Name = "TestModelChild2",
                    Value = 1,
                    FValue = 1.15f,
                    GValue = 1.25,
                    Timestamp= DateTime.Parse("2023-01-01"),
                    Parent = testModels[2],
                    Extra = testModelExtras[1]
                },
            };

            testModelGuidParents = new TestModelGuidParent[]
            {
                new TestModelGuidParent()
                {
                    Id = Guid.NewGuid(),
                    Name = "parent1"
                },
                new TestModelGuidParent()
                {
                    Id = Guid.NewGuid(),
                    Name = "parent2"
                }
           };

            testModelGuidChilds = new TestModelGuidChild[]
            {
                new TestModelGuidChild()
                {
                    Id = Guid.NewGuid(),
                    Name = "child1",
                    Parent = testModelGuidParents[0]
                },
                new TestModelGuidChild()
                {
                    Id = Guid.NewGuid(),
                    Name = "child2",
                    Parent = testModelGuidParents[1]
                }
            };

            testModelSimples = new TestModelSimple[]
          {
                new TestModelSimple()
                {
                    Id = 1,
                    Name = "a"
                },
                new TestModelSimple()
                {
                    Id = 2,
                    Name = "b"
                },
                new TestModelSimple()
                {
                    Id = 1,
                    Name = "c"
                },
                new TestModelSimple()
                {
                    Id = 3,
                    Name = "b"
                },
                new TestModelSimple()
                {
                    Id = null,
                    Name = "d"
                }
          };

            testModelParentCompositePrimaryKeys = new TestModelParentCompositePrimaryKey[]
        {
                new TestModelParentCompositePrimaryKey()
                {
                    i = 1,
                    j = 1,
                    k = "abc"
                },
                new TestModelParentCompositePrimaryKey()
                {
                    i = 1,
                    j = 2,
                    k = "def"
                },
                new TestModelParentCompositePrimaryKey()
                {
                    i = 2,
                    j = 2,
                    k = "ghi"
                },
                new TestModelParentCompositePrimaryKey()
                {
                    i = 3,
                    j = 3,
                    k = "jkl"
                },
        };

            testModelChildCompositePrimaryKeys = new TestModelChildCompositePrimaryKey[] {
                new TestModelChildCompositePrimaryKey()
                {
                    i = 1,
                    j = 1,
                    Parent = testModelParentCompositePrimaryKeys[0],
                    k = "abc"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 2,
                    j = 2,
                    Parent = testModelParentCompositePrimaryKeys[0],
                    k = "def"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 1,
                    j = 2,
                    Parent = testModelParentCompositePrimaryKeys[1],
                    k = "ghi"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 2,
                    j = 3,
                    Parent = testModelParentCompositePrimaryKeys[2],
                    k = "jkl"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 3,
                    j = 3,
                    Parent = testModelParentCompositePrimaryKeys[3],
                    k = "mno"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 4,
                    j = 1,
                    Parent = null,
                    k = "pqr"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 5,
                    j = 5,
                    Parent = testModelParentCompositePrimaryKeys[1],
                    k = "stu"
                }
            };

            testModelPrimaryKeyAsForeignKeys = new TestModelPrimaryKeyAsForeignKey[]
            {
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[0],
                    k = "abc"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[1],
                    k = "def"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[2],
                    k = "ghi"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[3],
                    k = "jkl"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[4],
                    k = "mno"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[5],
                    k = "pqr"
                }
            };

            testModelCompositePrimaryKeys = new TestModelCompositePrimaryKey[]
{
                new TestModelCompositePrimaryKey()
                {
                    i = 1,
                    j = 1,
                    k = "abc"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 2,
                    j = 2,
                    k = "def"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 1,
                    j = 2,
                    k = "ghi"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 2,
                    j = 3,
                    k = "jkl"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 3,
                    j = 3,
                    k = "mno"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 4,
                    j = 1,
                    k = "pqr"
                }
};
        }

        public void InsertTestData(IDataContext DataContext)
        {
            var testdata = new TestData();

            void ProcessInsertion<ModelT>(ModelT[] models) where ModelT : class, new()
            {
                foreach (var m in models)
                {
                    DataContext.Insert(m);
                }
            }
         ;

            ProcessInsertion(testdata.testModels);

            ProcessInsertion(testdata.testModelExtras);

            ProcessInsertion(testdata.testModelChilds);

            ProcessInsertion(testdata.testModelGuidParents);

            ProcessInsertion(testdata.testModelGuidChilds);

            ProcessInsertion(testdata.testModelSimples);

            ProcessInsertion(testdata.testModelParentCompositePrimaryKeys);

            ProcessInsertion(testdata.testModelChildCompositePrimaryKeys);

            ProcessInsertion(testdata.testModelPrimaryKeyAsForeignKeys);

            ProcessInsertion(testdata.testModelCompositePrimaryKeys);
        }
    }

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

            var mapper = (IDataContext context, object[] values) =>
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
            };

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

            switch (DataContext)
            {
                case MSSQL_DataContext _: TestContext.Out.WriteLine(new MSSQL_QueryParser().ToString(cmd)); break;
                case PostgreSQL_DataContext _: TestContext.Out.WriteLine(new PostgreSQL_QueryParser().ToString(cmd)); break;
                case SQLite_DataContext _: TestContext.Out.WriteLine(new SQLite_QueryParser().ToString(cmd)); break;
                case InMemory_SQLite_DataContext _: TestContext.Out.WriteLine(new SQLite_QueryParser().ToString(cmd)); break;
                default: throw new Exception($"{nameof(TestCreateTable)}. Unknown data context type: {DataContext.GetType()}");
            }

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

            switch (DataContext)
            {
                case MSSQL_DataContext _: TestContext.Out.WriteLine(new MSSQL_QueryParser().ToString(cmd)); break;
                case PostgreSQL_DataContext _: TestContext.Out.WriteLine(new PostgreSQL_QueryParser().ToString(cmd)); break;
                case SQLite_DataContext _: TestContext.Out.WriteLine(new SQLite_QueryParser().ToString(cmd)); break;
                case InMemory_SQLite_DataContext _: TestContext.Out.WriteLine(new SQLite_QueryParser().ToString(cmd)); break;
            }

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
            var check2 = result[0].Id == 1 && result[0].LongId == 1L && result[0].ShortId == 1 && result[0].Name == "TestModel1" && result[0].CharCode == "a" && result[0].Checked == false && result[0].Value == 1 && result[0].FValue == 1.1F && result[0].GValue == 1.2 && result[0].Money == (decimal)1.3 && result[0].Timestamp == DateTime.Parse("2024-01-01") && result[0].Duration == TimeSpan.Parse("23:59:59") && Convert.ToHexString(result[0].bindata) == "01020304";
            var check3 = result[1].Id == 2 && result[1].LongId == 2L && result[1].ShortId == 2 && result[1].Name == "TestModel2" && result[1].CharCode == "b" && result[1].Checked == true && result[1].Value == 1 && result[1].FValue == 1.1F && result[1].GValue == 1.2 && result[1].Money == (decimal)1.3 && result[1].Timestamp == DateTime.Parse("2024-03-01") && result[1].Duration == TimeSpan.Parse("01:01:01") && Convert.ToHexString(result[1].bindata) == "FFFFFFFF";
            var check4 = result[2].Id == 3 && result[2].LongId == null && result[2].ShortId == null && result[2].Name == "TestModel3" && result[2].CharCode == "b" && result[2].Checked == true && result[2].Value == 1 && result[2].FValue == 1.1F && result[2].GValue == 1.3 && result[2].Money == (decimal)1.4 && result[2].Timestamp == DateTime.Parse("2024-04-01") && result[2].Duration == TimeSpan.Parse("01:01:01") && Convert.ToHexString(result[2].bindata) == "0000FF00";

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
            var check2 = result[0].Id == 1 && result[0].LongId == 1L && result[0].ShortId == 1 && result[0].Name == "TestModel1" && result[0].CharCode == "a" && result[0].Checked == false && result[0].Value == 1 && result[0].FValue == 1.1F && result[0].GValue == 1.2 && result[0].Money == (decimal)1.3 && result[0].Timestamp == DateTime.Parse("2024-01-01") && result[0].Duration == TimeSpan.Parse("23:59:59") && Convert.ToHexString(result[0].bindata) == "01020304";
            var check3 = result[1].Id == 2 && result[1].LongId == 2L && result[1].ShortId == 2 && result[1].Name == "TestModel2" && result[1].CharCode == "b" && result[1].Checked == true && result[1].Value == 1 && result[1].FValue == 1.1F && result[1].GValue == 1.2 && result[1].Money == (decimal)1.3 && result[1].Timestamp == DateTime.Parse("2024-03-01") && result[1].Duration == TimeSpan.Parse("01:01:01") && Convert.ToHexString(result[1].bindata) == "FFFFFFFF";
            var check4 = result[2].Id == 3 && result[2].LongId == null && result[2].ShortId == null && result[2].Name == "TestModel3" && result[2].CharCode == "b" && result[2].Checked == true && result[2].Value == 1 && result[2].FValue == 1.1F && result[2].GValue == 1.3 && result[2].Money == (decimal)1.4 && result[2].Timestamp == DateTime.Parse("2024-04-01") && result[2].Duration == TimeSpan.Parse("01:01:01") && Convert.ToHexString(result[2].bindata) == "0000FF00";

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
            var cmd = DataContext.SelectFrom<TestModelSimple>().Where(m => m.Id >=2);
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
            var f = () => 1;
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
            TestContext.Out.WriteLine($"{ar.Length} {string.Join<TestModelSimple>(',', ar)}"); ;
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
                Convert.ToHexString(inserted.bindata) == Convert.ToHexString(m.bindata)
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

            var m = r.ToArray()[0];
            m.Name = "NewName";

            DataContext.Update(ModelMetadata<TestModelChild>.Instance, m);

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
            var s = Convert.ToHexString(new byte[] { 255, 255, 0, 255 });

            result[0].bindata = new byte[] { 255, 255, 0, 255 };

            DataContext.Update(result[0]);

            result = DataContext.Select<TestModel>(new SqlSelect().From<TestModel>().Where("Id", 1)).ToArray();

            Assert.That(s == Convert.ToHexString(result[0].bindata));
        }

        [Category("Update")]
        [Test]
        public void TestUpdateBinaryDynamic()
        {
            var result = DataContext.SelectFrom(ModelMetadata<TestModel>.Instance).Where("Id", 1).Select().ToArray();
            TestContext.Out.WriteLine(JsonConvert.SerializeObject(result));
            var s = Convert.ToHexString(new byte[] { 255, 255, 0, 255 });

            result[0].bindata = new byte[] { 255, 255, 0, 255 };

            DataContext.Update(ModelMetadata<TestModel>.Instance, result[0]);

            result = DataContext.Select(ModelMetadata<TestModel>.Instance, new SqlSelect().From(ModelMetadata<TestModel>.Instance).Where("Id", 1)).ToArray();

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
        [Test]
        public void TestCallTableFunctionWithoutParamDynamic()
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

            var result = DataContext.CallTableFunction(ModelMetadata<TestModel>.Instance, "dbo.testFunc");

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
        [TestCase(arguments: 0)]
        [TestCase(arguments: 1)]
        public void TestCallTableFunctionWithParamDynamic(int i)
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

            var result = DataContext.CallTableFunction(ModelMetadata<TestModel>.Instance, "dbo.testFuncParam", i);

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

    [TestFixture("sqlite", "Data Source=dbo;journal mode=off;synchronous=full;pooling=true")]
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

    [TestFixture("mssql", $"Data Source=localhost\\sqlexpress;Database=AdventureWorksLT2012;Integrated Security=True;Trust server certificate=true")]
    public class MSSQLTests : CommonTests<MSSQL_DataContext>
    {

        public override DataTools.Deploy.GeneratorBase GetGenerator()
        {
            return new MSSQL_Generator(this.DataContext.ConnectionString);
        }

        public override IDBMS_QueryParser GetQueryParser()
        {
            return new MSSQL_QueryParser();
        }

        public MSSQLTests(string alias, string connectionString) : base(alias)
        {
            this.alias = alias;
            DataContext.ConnectionString = connectionString;
        }

        public override void Teardown()
        {
            base.Teardown();

            var dc = DataManager.GetContext(alias) as MSSQL_DataContext;
            var ds = dc.GetDataSource() as MSSQL_DataSource;
        }
        public override void TeardownScripts()
        {

            base.TeardownScripts();

            DataContext.Execute(new SqlCustom("drop function if exists dbo.testFunc;"));
            DataContext.Execute(new SqlCustom("drop function if exists dbo.testFuncParam;"));
            DataContext.Execute(new SqlCustom("drop procedure if exists dbo.testProc;"));
            DataContext.Execute(new SqlCustom("drop procedure if exists dbo.testProcReturn;"));
        }

        public override void Setup()
        {
            base.Setup();

            var dc = DataManager.GetContext(alias) as MSSQL_DataContext;
            var ds = dc.GetDataSource() as MSSQL_DataSource;


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


            ds.Execute(@"
create function dbo.testFunc()
returns table
as
    return (select * from dbo.TestModel);
");

            ds.Execute(@"
create function dbo.testFuncParam(@i int)
returns table
as
    return (select * from dbo.TestModel where id = @i);
");
        }

        [Test]
        public void TestTypes()
        {
            TestType(true);
            TestType(1);
            TestType((byte)1);
            TestType((short)1);
            TestType((long)1);
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

            var boolean = new GenericWrapper<bool>(true);
            var result = DataContext.Select(ModelMetadata.CreateFromType(boolean.GetType()), new SqlCustom("SELECT CAST(1 AS bit) AS Value;")).First();
            Assert.That(result.Value is bool && result.Value == boolean.Value);

            var tinyIntValue = new GenericWrapper<byte>(250);
            result = DataContext.Select(ModelMetadata.CreateFromType(tinyIntValue.GetType()), new SqlCustom("SELECT CAST(250 AS tinyint) AS Value;")).First();
            Assert.That(result.Value is byte && result.Value == tinyIntValue.Value);

            var smallIntValue = new GenericWrapper<short>(32000);
            result = DataContext.Select(ModelMetadata.CreateFromType(smallIntValue.GetType()), new SqlCustom("SELECT CAST(32000 AS smallint) AS Value;")).First();
            Assert.That(result.Value is short && result.Value == smallIntValue.Value);

            var intValue = new GenericWrapper<int>(1000);
            result = DataContext.Select(ModelMetadata.CreateFromType(intValue.GetType()), new SqlCustom("SELECT CAST(1000 AS int) AS Value;")).First();
            Assert.That(result.Value is int && result.Value == intValue.Value);

            var bigIntValue = new GenericWrapper<long>(1000000000);
            result = DataContext.Select(ModelMetadata.CreateFromType(bigIntValue.GetType()), new SqlCustom("SELECT CAST(1000000000 AS bigint) AS Value;")).First();
            Assert.That(result.Value is long && result.Value == bigIntValue.Value);

            var decimalValue = new GenericWrapper<decimal>(123.45m);
            result = DataContext.Select(ModelMetadata.CreateFromType(decimalValue.GetType()), new SqlCustom("SELECT CAST(123.45 AS decimal(10,2)) AS Value;")).First();
            Assert.That(result.Value is decimal && result.Value == decimalValue.Value);

            var floatValue = new GenericWrapper<float>(123.45f);
            result = DataContext.Select(ModelMetadata.CreateFromType(floatValue.GetType()), new SqlCustom("SELECT CAST(123.45 AS real) AS Value;")).First();
            Assert.That(result.Value is float && result.Value == floatValue.Value);

            var doubleValue = new GenericWrapper<double>(123.45);
            result = DataContext.Select(ModelMetadata.CreateFromType(doubleValue.GetType()), new SqlCustom("SELECT CAST(123.45 AS float) AS Value;")).First();
            Assert.That(result.Value is double && result.Value == doubleValue.Value);

            var varcharValue = new GenericWrapper<string>("Hello, World!");
            result = DataContext.Select(ModelMetadata.CreateFromType(varcharValue.GetType()), new SqlCustom("SELECT CAST('Hello, World!' AS varchar(50)) AS Value;")).First();
            Assert.That(result.Value is string && result.Value == varcharValue.Value);

            var nvarcharValue = new GenericWrapper<string>("Hello, World!");
            result = DataContext.Select(ModelMetadata.CreateFromType(nvarcharValue.GetType()), new SqlCustom("SELECT CAST('Hello, World!' AS nvarchar(50)) AS Value;")).First();
            Assert.That(result.Value is string && result.Value == nvarcharValue.Value);

            var charValue = new GenericWrapper<string>("a");
            result = DataContext.Select(ModelMetadata.CreateFromType(charValue.GetType()), new SqlCustom("SELECT CAST('a' AS char(1)) AS Value;")).First();
            Assert.That(result.Value is string && result.Value == charValue.Value);

            var textValue = new GenericWrapper<string>("Hello, World!");
            result = DataContext.Select(ModelMetadata.CreateFromType(textValue.GetType()), new SqlCustom("SELECT CAST('Hello, World!' AS text) AS Value;")).First();
            Assert.That(result.Value is string && result.Value == textValue.Value);

            var ntextValue = new GenericWrapper<string>("Hello, World!");
            result = DataContext.Select(ModelMetadata.CreateFromType(ntextValue.GetType()), new SqlCustom("SELECT CAST('Hello, World!' AS ntext) AS Value;")).First();
            Assert.That(result.Value is string && result.Value == ntextValue.Value);

            var dateValue = new GenericWrapper<DateTime>(DateTime.Parse("2025-08-07"));
            result = DataContext.Select(ModelMetadata.CreateFromType(dateValue.GetType()), new SqlCustom("SELECT CAST('2025-08-07' AS date) AS Value;")).First();
            Assert.That(result.Value is DateTime && result.Value == dateValue.Value);

            var datetimeValue = new GenericWrapper<DateTime>(DateTime.Parse("2025-08-07 15:27:36"));
            result = DataContext.Select(ModelMetadata.CreateFromType(datetimeValue.GetType()), new SqlCustom("SELECT CAST({ts '2025-08-07 15:27:36'} AS datetime) AS Value;")).First();
            Assert.That(result.Value is DateTime && result.Value == datetimeValue.Value);

            var datetimeoffsetValue = new GenericWrapper<DateTimeOffset>(DateTimeOffset.Parse("2025-08-07 15:27:36"));
            result = DataContext.Select(ModelMetadata.CreateFromType(datetimeoffsetValue.GetType()), new SqlCustom("SELECT CAST({ts '2025-08-07 08:27:36'} AS datetimeoffset) AS Value;")).First();
            Assert.That(result.Value is DateTimeOffset && result.Value.ToUniversalTime() == datetimeoffsetValue.Value.ToUniversalTime());

            var datetime2Value = new GenericWrapper<DateTime>(DateTime.Parse("2025-08-07 15:27:36"));
            result = DataContext.Select(ModelMetadata.CreateFromType(datetime2Value.GetType()), new SqlCustom("SELECT CAST({ts '2025-08-07 15:27:36'} AS datetime2) AS Value;")).First();
            Assert.That(result.Value is DateTime && result.Value == datetime2Value.Value);

            var timeValue = new GenericWrapper<TimeSpan>(TimeSpan.Parse("15:27:36"));
            result = DataContext.Select(ModelMetadata.CreateFromType(timeValue.GetType()), new SqlCustom("SELECT CAST({t '15:27:36'} AS time) AS Value;")).First();
            Assert.That(result.Value is TimeSpan && result.Value == timeValue.Value);

            var binary = new GenericWrapper<byte[]>(new byte[] { 1, 2, 3, 4 });
            result = DataContext.Select(ModelMetadata.CreateFromType(binary.GetType()), new SqlCustom("SELECT CAST(0x01020304 AS binary(4)) AS Value;")).First();
            Assert.That(result.Value is byte[] && BitConverter.ToString(result.Value) == BitConverter.ToString(binary.Value));

            var varbinary = new GenericWrapper<byte[]>(new byte[] { 1, 2, 3, 4 });
            result = DataContext.Select(ModelMetadata.CreateFromType(varbinary.GetType()), new SqlCustom("SELECT CAST(0x01020304 AS varbinary(4)) AS Value;")).First();
            Assert.That(result.Value is byte[] && BitConverter.ToString(result.Value) == BitConverter.ToString(varbinary.Value));

            var image = new GenericWrapper<byte[]>(new byte[] { 1, 2, 3, 4 });
            result = DataContext.Select(ModelMetadata.CreateFromType(image.GetType()), new SqlCustom("SELECT CAST(0x01020304 AS image) AS Value;")).First();
            Assert.That(result.Value is byte[] && BitConverter.ToString(result.Value) == BitConverter.ToString(image.Value));

            var guid = new GenericWrapper<Guid>(Guid.Parse("8d5cbb93-28b3-46c0-8c37-e973b3d772a4"));
            result = DataContext.Select(ModelMetadata.CreateFromType(guid.GetType()), new SqlCustom("SELECT CAST('8d5cbb93-28b3-46c0-8c37-e973b3d772a4' AS uniqueidentifier) AS Value;")).First();
            Assert.That(result.Value is Guid && result.Value == guid.Value);

            var xml = new GenericWrapper<string>("<root><child>Значение</child></root>");
            result = DataContext.Select(ModelMetadata.CreateFromType(xml.GetType()), new SqlCustom("SELECT CAST(N'<root><child>Значение</child></root>' AS xml) AS Value;")).First();
            Assert.That(result.Value is string && result.Value == xml.Value);
        }

    }

    [TestFixture(
        new E_DBMS[] { E_DBMS.MSSQL, E_DBMS.PostgreSQL, E_DBMS.SQLite },
        new string[] {
        $"Data Source=localhost\\sqlexpress;Database=test;Integrated Security=True;Trust server certificate=true",
        "Username=postgres;Password=1qaz@WSX;Host=localhost;Database=test",
        "Data Source=dbo;journal mode=off;synchronous=full;pooling=true"},
        new string[] {
        $"Data Source=localhost\\sqlexpress;Database=test1;Integrated Security=True;Trust server certificate=true",
        "Username=postgres;Password=1qaz@WSX;Host=localhost;Database=test1",
        "Data Source=dbo1;journal mode=off;synchronous=full;pooling=true"}
        )]
    public class MultiTest
    {
        private E_DBMS[] _types;
        private string[] _fromConnectionStrings;
        private string[] _toConnectionStrings;

        private TestData testdata = new TestData();

        public MultiTest(E_DBMS[] types, string[] fromConnectionStrings, string[] toConnectionStrings)
        {
            _types = types;
            _fromConnectionStrings = fromConnectionStrings;
            _toConnectionStrings = toConnectionStrings;
        }

        [Test]
        public void TestMigrations()
        {
            int i = 0;
            foreach (var t in _types)
            {
                var deployerOptions = new DeployerOptions()
                {
                    DBMS = t,
                    ConnectionString = _fromConnectionStrings[i],
                    Metadatas = testdata.Metadatas,
                    Mode = E_DEPLOY_MODE.REDEPLOY
                };
                var deployer = new DataTools.Deploy.DeployerWorker(deployerOptions);
                deployer.Run();
                TestContext.Out.WriteLine($"{deployerOptions.DBMS} deployed");

                testdata.InsertTestData(deployer.DataContext);

                var generatorOptions = new GeneratorOptions()
                {
                    DBMS = t,
                    ConnectionString = _fromConnectionStrings[i],
                    NamespaceName = "Test",
                    SchemaIncludeNameFilter = "dbo",
                    TableIncludeNameFilter = ""
                };
                var generator = new DataTools.Deploy.GeneratorWorker(generatorOptions);
                var metadatas = generator.GetModelDefinitions().ToArray();
                TestContext.Out.WriteLine($"{deployerOptions.DBMS} generated");

                int j = 0;
                foreach (var tt in _types)
                {
                    var deployerOptions1 = new DeployerOptions()
                    {
                        DBMS = tt,
                        ConnectionString = _toConnectionStrings[j],
                        IgnoreIdentities = true,
                        Metadatas = metadatas.Select(mt => mt.ModelMetadata),
                        Mode = E_DEPLOY_MODE.REDEPLOY
                    };
                    var deployer1 = new DataTools.Deploy.DeployerWorker(deployerOptions1);
                    deployer1.Run();
                    TestContext.Out.WriteLine($"From {deployerOptions.DBMS} to {deployerOptions1.DBMS} deployed");

                    var migratorOptions = new DataMigrationOptions()
                    {
                        FromDBMS = t,
                        ToDBMS = tt,
                        FromConnectionString = _fromConnectionStrings[i],
                        ToConnectionString = _toConnectionStrings[j],
                        IgnoreConstraints = true,
                        Metadatas = metadatas.Select(mt => mt.ModelMetadata)
                    };
                    var migrator = new DataMigrationWorker(migratorOptions);
                    migrator.Run();
                    TestContext.Out.WriteLine($"From {deployerOptions.DBMS} to {deployerOptions1.DBMS} migrated");

                    //deployer1.Mode = E_DEPLOY_MODE.RESTORE_IDENTITIES;
                    //deployer1.Run();
                    //TestContext.Out.WriteLine($"{deployerOptions1.DBMS} identites restored");

                    foreach (var meta in testdata.Metadatas)
                    {
                        TestContext.Out.WriteLine($"{meta.FullObjectName} compare start.");

                        var mapperMethod = typeof(ModelMapper<>).MakeGenericType(Type.GetType(meta.ModelTypeName)).GetProperty("MapModel");

                        var orderArray = meta.GetColumnsForOrdering().ToArray();
                        if (orderArray.Length == 0)
                            orderArray = meta.GetColumnsForSelect().ToArray();

                        var leftData = deployer.DataContext.ExecuteWithResult(new SqlSelect().From(meta).OrderBy(orderArray)).ToArray();
                        var leftModels = new object[leftData.Length];
                        var rightData = deployer1.DataContext.ExecuteWithResult(new SqlSelect().From(meta).OrderBy(orderArray)).ToArray();
                        var rightModels = new object[rightData.Length];

                        leftData = leftData.OrderBy(r => System.Text.Json.JsonSerializer.Serialize(r)).ToArray();
                        rightData = rightData.OrderBy(r => System.Text.Json.JsonSerializer.Serialize(r)).ToArray();


                        Assert.That(leftData.Length == rightData.Length);

                        var sc = new SelectCache();

                        Dictionary<Type, Func<object, object>> _customTypeConverters = new Dictionary<Type, Func<object, object>>();

                        for (int i1 = 0; i1 < leftData.Length; i1++)
                        {
                            object[]? r = leftData[i1];
                            var o = (mapperMethod.GetValue(null) as Delegate).DynamicInvoke(new object[] { deployer.DataContext, _customTypeConverters, r, sc });
                            leftModels[i1] = o;
                            object[]? rr = rightData[i1];
                            o = (mapperMethod.GetValue(null) as Delegate).DynamicInvoke(new object[] { deployer.DataContext, _customTypeConverters, r, sc });
                            rightModels[i1] = o;
                        }

                        for (int i1 = 0; i1 < leftData.Length; i1++)
                        {
                            var props = leftModels[i1].GetType().GetProperties();
                            foreach (var pr in props)
                            {
                                var leftv = pr.GetValue(leftModels[i1]);
                                var rightv = pr.GetValue(rightModels[i1]);

                                if (leftv == null && rightv == null)
                                    Assert.That(true);
                                else if (meta.GetField(pr.Name).IsForeignKey)
                                {
                                    for (int i2 = 0; i2 < meta.GetField(pr.Name).ForeignColumnNames.Length; i2++)
                                    {
                                        string? fc = meta.GetField(pr.Name).ForeignColumnNames[i2];
                                        var leftfv = Type.GetType(meta.GetField(pr.Name).ForeignModel.ModelTypeName).GetProperty(meta.GetField(pr.Name).ForeignModel.GetColumn(fc).FieldName).GetValue(leftv);
                                        var rightfv = Type.GetType(meta.GetField(pr.Name).ForeignModel.ModelTypeName).GetProperty(meta.GetField(pr.Name).ForeignModel.GetColumn(fc).FieldName).GetValue(rightv);
                                        if (leftfv is byte[] bytea)
                                            Assert.That(BitConverter.ToString(bytea) == BitConverter.ToString(rightfv as byte[]));
                                        else if (leftfv is DateTime dateTime)
                                            Assert.That(dateTime.ToUniversalTime() == ((DateTime)rightfv).ToUniversalTime());
                                        else if (leftfv is DateTimeOffset dateTimeOffset)
                                            Assert.That(dateTimeOffset.ToUniversalTime() == ((DateTimeOffset)rightfv).ToUniversalTime());
                                        else
                                            Assert.That(leftfv.Equals(rightfv));
                                    }
                                }
                                else if (leftv is byte[] bytea)
                                    Assert.That(BitConverter.ToString(bytea) == BitConverter.ToString(rightv as byte[]));
                                else if (leftv is DateTime dateTime)
                                    Assert.That(dateTime.ToUniversalTime() == ((DateTime)rightv).ToUniversalTime());
                                else if (leftv is DateTimeOffset dateTimeOffset)
                                    Assert.That(dateTimeOffset.ToUniversalTime() == ((DateTimeOffset)rightv).ToUniversalTime());
                                else
                                    Assert.That(leftv.Equals(rightv));

                            }
                        }
                        TestContext.Out.WriteLine($"{meta.FullObjectName} compare finish!");
                    }

                    j++;
                }

                i++;
            }
        }

        [TearDown]
        public void Teardown()
        {

        }
    }


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
        [Reference(foreignFieldNames: [nameof(TestModel.Id)], columnNames: ["Parent_Id"])]
        public TestModel Parent { get; set; }
        [Reference(foreignFieldNames: [nameof(TestModelExtra.Id)], columnNames: ["Extra_id"])]
        public TestModelExtra Extra { get; set; }

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
        [Reference(foreignFieldNames: [nameof(TestModelExtra.Id)], columnNames: ["Extra_id"])]
        public TestModelExtra Extra { get; set; }
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
        [Reference(foreignFieldNames: [nameof(TestModelGuidParent.Id)], columnNames: ["parent_id"])]
        public TestModelGuidParent Parent { get; set; }
    }

    [ObjectName("TestModelNoUnique", "dbo")]
    [NoUniqueAttribute]
    public class TestModelSimple
    {
        //[Unique]
        public int? Id { get; set; }
        public string Name { get; set; }
    }

    [ObjectName(nameof(TestModelParentCompositePrimaryKey), "dbo")]
    public class TestModelParentCompositePrimaryKey
    {
        [PrimaryKey]
        public int i { get; set; }
        [PrimaryKey]
        public int j { get; set; }

        public string k { get; set; }
    }

    [ObjectName(nameof(TestModelChildCompositePrimaryKey), "dbo")]
    public class TestModelChildCompositePrimaryKey
    {
        [PrimaryKey]
        public int i { get; set; }
        [PrimaryKey]
        public int j { get; set; }

        [Reference(foreignFieldNames: new string[] { nameof(TestModelParentCompositePrimaryKey.i), nameof(TestModelParentCompositePrimaryKey.j) }, columnNames: new string[] { "Parent_i", "Parent_j" })]
        public TestModelParentCompositePrimaryKey Parent { get; set; }

        public string k { get; set; }
    }

    [ObjectName(nameof(TestModelCompositePrimaryKey), "dbo")]
    public class TestModelCompositePrimaryKey
    {
        [PrimaryKey]
        public int i { get; set; }
        [PrimaryKey]
        public int j { get; set; }
        public string k { get; set; }
    }


    [ObjectName(nameof(TestModelPrimaryKeyAsForeignKey), "dbo")]
    public class TestModelPrimaryKeyAsForeignKey
    {
        [PrimaryKey]
        [Reference(foreignFieldNames: new string[] { nameof(TestModelChildCompositePrimaryKey.i), nameof(TestModelChildCompositePrimaryKey.j) }, columnNames: new string[] { "Child_i", "Child_j" })]
        public TestModelChildCompositePrimaryKey Child { get; set; }

        public string k { get; set; }
    }
}

