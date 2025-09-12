using DataTools.Common;
using DataTools.Deploy;
using DataTools.DML;
using DataTools.Extensions;
using NUnit.Framework.Internal;

namespace DataTools_Tests
{
    [TestFixture(
        new E_DBMS[] { E_DBMS.MSSQL, E_DBMS.PostgreSQL, E_DBMS.SQLite },
        new string[] {
        $"Data Source=localhost\\sqlexpress;Database=test;Integrated Security=True;Trust server certificate=true",
        "Username=postgres;Password=1qaz@WSX;Host=localhost;Database=test",
        "Data Source=dbo;pooling=true"},
        new string[] {
        $"Data Source=localhost\\sqlexpress;Database=test1;Integrated Security=True;Trust server certificate=true",
        "Username=postgres;Password=1qaz@WSX;Host=localhost;Database=test1",
        "Data Source=dbo1;pooling=true"}
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

                    foreach (var meta in testdata.Metadatas)
                    {
                        TestContext.Out.WriteLine($"{meta.FullObjectName} compare start.");

                        var mapperMethod = typeof(ModelMapper<>).MakeGenericType(Type.GetType(meta.ModelTypeName)).GetProperty(nameof(ModelMapper<TestModel>.MapObjectArrayToModel));

                        var orderArray = meta.GetColumnsForFilterOrder().ToArray();
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
                            object m = Activator.CreateInstance(Type.GetType(meta.ModelTypeName));
                            var o = (mapperMethod.GetValue(null) as Delegate).DynamicInvoke(new object[] {m, deployer.DataContext, _customTypeConverters, r, sc });
                            leftModels[i1] = m;
                            object[]? rr = rightData[i1];
                            m = Activator.CreateInstance(Type.GetType(meta.ModelTypeName));
                            o = (mapperMethod.GetValue(null) as Delegate).DynamicInvoke(new object[] {m, deployer.DataContext, _customTypeConverters, r, sc });
                            rightModels[i1] = m;
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
}

