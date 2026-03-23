using DataTools.Deploy;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.SQLite;
using Microsoft.Data.Sqlite;
using NUnit.Framework.Internal;

namespace DataTools_Tests
{
    [TestFixture("sqlite", "Data Source=dbo.db;pooling=true")]
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

        private SqliteConnection _conn;

        public SQLiteTests(string alias, string connectionString) : base(alias)
        {
            this.alias = alias;
            DataContext.ConnectionString = connectionString;
            //_conn = new SqliteConnection(connectionString);
            //_conn.Open();
        }

        [SetUp]
        public override void Setup()
        {
            DataContext.Execute(new SqlCustom("PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF; pragma temp_store = memory;"));
            base.Setup();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            //_conn.Close();
        }
    }
}

