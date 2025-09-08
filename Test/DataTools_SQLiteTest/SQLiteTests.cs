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
    }
}

