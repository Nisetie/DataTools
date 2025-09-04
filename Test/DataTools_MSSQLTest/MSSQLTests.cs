using DataTools.Common;
using DataTools.Deploy;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using DataTools.MSSQL;
using NUnit.Framework.Internal;

namespace DataTools_Tests
{
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
        public void TestTypesMSSQL()
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
}

