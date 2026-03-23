using BenchmarkDotNet.Attributes;
using DataTools.Attributes;
using DataTools.Common;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.InMemory_SQLite;
using DataTools.Interfaces;
using DataTools.Meta;
using DataTools.MSSQL;
using DataTools.SQLite;
using Microsoft.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using System.Text;
using SqlParameter = DataTools.DML.SqlParameter;

namespace DataTools_Benchmarks
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //BenchmarkDotNet.Running.BenchmarkRunner.Run<DataTools_Benchs>(BenchmarkDotNet.Configs.DefaultConfig.Instance.WithOption(ConfigOptions.DisableOptimizationsValidator, true));


            var sw = new Stopwatch();
            var b = new DataTools_Benchs();

            b.Setup();

            while (true)
            {
                Console.Clear();
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                sw.Restart();
                b.DoMSSQLSelectRecord();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectRecord)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoMSSQLSelectManyRecords();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoMSSQLSelectRecordManyTimes();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoMSSQLSelectManyRecordsWithReferences();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoMSSQLSelectManyRecordsWithReferencesOptimized();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectManyRecordsWithReferencesOptimized)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                sw.Restart();
                b.DoEntityFrameworkSelectRecord();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectRecord)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoEntityFrameworkSelectManyRecords();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoEntityFrameworkSelectRecordManyTimes();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoEntityFrameworkSelectManyRecordsWithReferences();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                sw.Restart();
                b.DoDapperSelectRecord();
                Console.WriteLine($"Dapper {nameof(DataTools_Benchs.DoDapperSelectRecord)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDapperSelectManyRecords();
                Console.WriteLine($"Dapper {nameof(DataTools_Benchs.DoDapperSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDapperSelectRecordManyTimes();
                Console.WriteLine($"Dapper {nameof(DataTools_Benchs.DoDapperSelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDapperSelectManyRecordsWithReferences();
                Console.WriteLine($"Dapper {nameof(DataTools_Benchs.DoDapperSelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                sw.Restart();
                b.DoDataToolsMSSQLSelectRecord();
                Console.WriteLine($"DataTools MSSQL {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectRecord)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectManyRecords();
                Console.WriteLine($"DataTools MSSQL {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectRecordManyTimes();
                Console.WriteLine($"DataTools MSSQL {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectManyRecordsWithReferences();
                Console.WriteLine($"DataTools MSSQL {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                sw.Restart();
                b.DoDataToolsInMemorySelectRecord();
                Console.WriteLine($"DataTools InMemory {nameof(DataTools_Benchs.DoDataToolsInMemorySelectRecord)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsInMemorySelectManyRecords();
                Console.WriteLine($"DataTools InMemory {nameof(DataTools_Benchs.DoDataToolsInMemorySelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsInMemorySelectRecordManyTimes();
                Console.WriteLine($"DataTools InMemory {nameof(DataTools_Benchs.DoDataToolsInMemorySelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsInMemorySelectManyRecordsWithReferences();
                Console.WriteLine($"DataTools InMemory {nameof(DataTools_Benchs.DoDataToolsInMemorySelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();

                sw.Restart();
                b.DoDataToolsMSSQLSelectRecordDynamic();
                Console.WriteLine($"DataTools MSSQL Dynamic {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectRecordDynamic)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectManyRecordsDynamic();
                Console.WriteLine($"DataTools MSSQL Dynamic {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectManyRecordsDynamic)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectRecordManyTimesDynamic();
                Console.WriteLine($"DataTools MSSQL Dynamic {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectRecordManyTimesDynamic)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectManyRecordsWithReferencesDynamic();
                Console.WriteLine($"DataTools MSSQL Dynamic {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectManyRecordsWithReferencesDynamic)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);


                Console.WriteLine("Again? Press Y...");
                if (Console.ReadKey().Key != ConsoleKey.Y) break;
            }

        }


    }

    [MemoryDiagnoser]
    public class DataTools_Benchs
    {
        [Params(1, 1000, 10000)]
        public int c = 10000;
        private string connectionString = "Data Source=(LocalDB)\\MSSQLLocalDB;";
        private IDataContext _mssqlcontext;
        private IDataContext _inmemorycontext = new InMemory_SQLite_DataContext();
        private SqlConnection _connection;


        [GlobalSetup]
        public void Setup()
        {
            cachedSelect_test_simplified = new MSSQL_QueryParser().SimplifyQuery(cachedSelect_test);
            cachedSelect_testt_MSSQLSimplified = new MSSQL_QueryParser().SimplifyQuery(cachedSelect_testt);

            var context = DataManager.AddContext("mssql", new MSSQL_DataContext()) as MSSQL_DataContext;
            context.ConnectionString = connectionString;
            _mssqlcontext = context;
            _connection = new SqlConnection(connectionString);

            var ds = _mssqlcontext.GetDataSource() as MSSQL_DataSource;
            ds.Execute(@"
drop table if exists testt;
drop table if exists test;
create table test (i int primary key
, LongId bigint,
    ShortId smallint,
    Name NVARCHAR (MAX),
    CharCode char,
    Checked bit,
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Money decimal,
    Timestamp DATETIME,
    Duration time,
    Guid uniqueidentifier
);
with t1 as (
    select 1 i union all select i+1 from t1 where t1.i < " + c.ToString() + @"
)
insert into test(i,LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid) 
select i, 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59',newid() from t1
option (maxrecursion 0);
");

            ds.Execute(@"

create table testt (i int primary key
, LongId bigint,
    ShortId smallint,
    Name NVARCHAR (MAX),
    CharCode char,
    Checked bit,
    Value INT,
    FValue REAL,
    GValue FLOAT,
    Money decimal,
    Timestamp DATETIME,
    Duration time,
    Guid uniqueidentifier,
    testi int references test(i)
);

with t1 as (
    select 1 i union all select i+1 from t1 where t1.i < " + c.ToString() + @"
)
insert into testt(i,LongId,ShortId,Name,CharCode,Checked,Value,FValue,GValue,Money,Timestamp,Duration,Guid,testi) 
select i, 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59',newid(),1 + ((i-1) % 5) from t1
--select i, 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59',newid(),i from t1
option (maxrecursion 0);
");

            _inmemorycontext.CreateTable<test>();

            var i = new SqlParameter("i");
            var LongId = new SqlParameter("LongId");
            var ShortId = new SqlParameter("ShortId");
            var Name = new SqlParameter("Name");
            var CharCode = new SqlParameter("CharCode");
            var Checked = new SqlParameter("Checked");
            var Value = new SqlParameter("Value");
            var FValue = new SqlParameter("FValue");
            var GValue = new SqlParameter("GValue");
            var Money = new SqlParameter("Money");
            var Timestamp = new SqlParameter("Timestamp");
            var Duration = new SqlParameter("Duration");
            var GUID = new SqlParameter("Guid");
            var test = new SqlParameter("test");

            var insert = new SqlInsert();
            insert.Into<test>().Value(i, LongId, ShortId, Name, CharCode, Checked, Value, FValue, GValue, Money, Timestamp, Duration, GUID);

            for (int j = 0; j < c; ++j)
            {
                i.Value = j + 1;
                LongId.Value = 1;
                ShortId.Value = 1;
                Name.Value = "TestModel1";
                CharCode.Value = "a";
                Checked.Value = true;
                Value.Value = 1;
                FValue.Value = 1.1F;
                GValue.Value = 1.2;
                Money.Value = (decimal)1.3;
                Timestamp.Value = DateTime.Parse("2024-01-01");
                Duration.Value = TimeSpan.Parse("23:59:59");
                GUID.Value = Guid.NewGuid();

                _inmemorycontext.Execute(insert, i, LongId, ShortId, Name, CharCode, Checked, Value, FValue, GValue, Money, Timestamp, Duration, GUID);
            }

            insert.Into<testt>().Value(i, LongId, ShortId, Name, CharCode, Checked, Value, FValue, GValue, Money, Timestamp, Duration, GUID, test);

            var testId = new SqlParameter("testId");
            var select = _inmemorycontext.Select<test>(new SqlSelect().From<test>().Where(new SqlWhere().Name("i").Eq(testId)), testId);

            _inmemorycontext.CreateTable<testt>();
            for (int j = 0; j < c; ++j)
            {

                testId.Value = 1 + (j % 5);

                i.Value = j + 1;
                LongId.Value = 1;
                ShortId.Value = 1;
                Name.Value = "TestModel1";
                CharCode.Value = "a";
                Checked.Value = true;
                Value.Value = 1;
                FValue.Value = 1.1F;
                GValue.Value = 1.2;
                Money.Value = (decimal)1.3;
                Timestamp.Value = DateTime.Parse("2024-01-01");
                Duration.Value = TimeSpan.Parse("23:59:59");
                GUID.Value = Guid.NewGuid();
                test.Value = select.First().i;

                _inmemorycontext.Execute(insert, i, LongId, ShortId, Name, CharCode, Checked, Value, FValue, GValue, Money, Timestamp, Duration, GUID, test);
            }
        }

        [Benchmark]
        public void DoMSSQLSelectRecord()
        {
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"select top 1 * from dbo.test where i={c}";
            var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
            var l = new List<test>();
            var test = new test();
            while (reader.Read())
            {
                test.i = (int)reader["i"];
                test.LongId = (long)reader["LongId"];
                test.ShortId = (short)reader["ShortId"];
                test.Name = (string)reader["Name"];
                test.CharCode = (string)reader["CharCode"];
                test.Checked = (bool)reader["Checked"];
                test.Value = (int)reader["Value"];
                test.FValue = (float)reader["FValue"];
                test.GValue = (double)reader["GValue"];
                test.Money = (decimal)reader["Money"];
                test.Timestamp = (DateTime)reader["Timestamp"];
                test.Duration = (TimeSpan)reader["Duration"];
                test.Guid = (Guid)reader["Guid"];
                l.Add(test);
            }
            reader.Close();
            conn.Close();
        }

        [Benchmark]
        public void DoMSSQLSelectManyRecords()
        {
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from test";
            var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
            var l = new List<test>();
            var test = new test();
            while (reader.Read())
            {
                test.i = (int)reader["i"];
                test.LongId = (long)reader["LongId"];
                test.ShortId = (short)reader["ShortId"];
                test.Name = (string)reader["Name"];
                test.CharCode = (string)reader["CharCode"];
                test.Checked = (bool)reader["Checked"];
                test.Value = (int)reader["Value"];
                test.FValue = (float)reader["FValue"];
                test.GValue = (double)reader["GValue"];
                test.Money = (decimal)reader["Money"];
                test.Timestamp = (DateTime)reader["Timestamp"];
                test.Duration = (TimeSpan)reader["Duration"];
                test.Guid = (Guid)reader["Guid"];
                l.Add(test);
            }
            reader.Close();
            conn.Close();
        }

        [Benchmark]
        public void DoMSSQLSelectManyRecordsWithReferences()
        {
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from testt";
            var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
            var l = new List<testt>();
            var test = new testt();
            while (reader.Read())
            {
                test.i = (int)reader["i"];
                test.LongId = (long)reader["LongId"];
                test.ShortId = (short)reader["ShortId"];
                test.Name = (string)reader["Name"];
                test.CharCode = (string)reader["CharCode"];
                test.Checked = (bool)reader["Checked"];
                test.Value = (int)reader["Value"];
                test.FValue = (float)reader["FValue"];
                test.GValue = (double)reader["GValue"];
                test.Money = (decimal)reader["Money"];
                test.Timestamp = (DateTime)reader["Timestamp"];
                test.Duration = (TimeSpan)reader["Duration"];
                test.Guid = (Guid)reader["Guid"];
                l.Add(test);

                SqlConnection conn1 = new SqlConnection(connectionString);
                conn1.Open();
                var cmd1 = conn1.CreateCommand();
                cmd1.CommandText = "select * from test where i = " + reader["testi"].ToString();
                var reader1 = cmd1.ExecuteReader();
                reader1.Read();
                test.test = new test()
                {
                    i = (int)reader1["i"],
                    LongId = (long)reader1["LongId"],
                    ShortId = (short)reader1["ShortId"],
                    Name = (string)reader1["Name"],
                    CharCode = (string)reader1["CharCode"],
                    Checked = (bool)reader1["Checked"],
                    Value = (int)reader1["Value"],
                    FValue = (float)reader1["FValue"],
                    GValue = (double)reader1["GValue"],
                    Money = (decimal)reader1["Money"],
                    Timestamp = (DateTime)reader1["Timestamp"],
                    Duration = (TimeSpan)reader1["Duration"],
                    Guid = (Guid)reader1["Guid"]
                };
                reader1.Close(); conn1.Close();

            }
            reader.Close();
            conn.Close();
        }

        [Benchmark]
        public void DoMSSQLSelectManyRecordsWithReferencesOptimized()
        {
            var cmdStr = new StringBuilder("select * from test where i = ");
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            SqlConnection conn1 = new SqlConnection(connectionString);
            conn1.Open();
            var cmd1 = conn1.CreateCommand();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from testt";
            var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
            var l = new List<testt>();
            var ll = new Dictionary<int, test>();
            var test = new testt();
            while (reader.Read())
            {
                test.i = (int)reader[0];
                test.LongId = (long)reader[1];
                test.ShortId = (short)reader[2];
                test.Name = (string)reader[3];
                test.CharCode = (string)reader[4];
                test.Checked = (bool)reader[5];
                test.Value = (int)reader[6];
                test.FValue = (float)reader[7];
                test.GValue = (double)reader[8];
                test.Money = (decimal)reader[9];
                test.Timestamp = (DateTime)reader[10];
                test.Duration = (TimeSpan)reader[11];
                test.Guid = (Guid)reader[12];
                l.Add(test);

                int i = ((int)reader[13]);

                if (!ll.TryGetValue(i, out var value))
                {
                    cmd1.CommandText = $"{cmdStr}{i}";
                    var reader1 = cmd1.ExecuteReader(System.Data.CommandBehavior.SingleRow);
                    if (reader1.Read())
                        test.test = new test()
                        {
                            i = (int)reader1[0],
                            LongId = (long)reader1[1],
                            ShortId = (short)reader1[2],
                            Name = (string)reader1[3],
                            CharCode = (string)reader1[4],
                            Checked = (bool)reader1[5],
                            Value = (int)reader1[6],
                            FValue = (float)reader1[7],
                            GValue = (double)reader1[8],
                            Money = (decimal)reader1[9],
                            Timestamp = (DateTime)reader1[10],
                            Duration = (TimeSpan)reader1[11],
                            Guid = (Guid)reader1[12]
                        };
                    ll[i] = test.test;
                    reader1.Close(); //conn1.Close();

                }
                else
                {
                    test.test = value;
                }

            }
            reader.Close();
            conn.Close(); conn1.Close();
        }

        [Benchmark]
        public void DoMSSQLSelectRecordManyTimes()
        {
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from test where i = @i";
            cmd.Parameters.Add("i", System.Data.SqlDbType.Int);
            var test = new test();
            for (int i = 0; i < c; ++i)
            {
                cmd.Parameters["i"].Value = i + 1;
                var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
                if (!reader.Read()) continue;
                test.i = (int)reader["i"];
                test.LongId = (long)reader["LongId"];
                test.ShortId = (short)reader["ShortId"];
                test.Name = (string)reader["Name"];
                test.CharCode = (string)reader["CharCode"];
                test.Checked = (bool)reader["Checked"];
                test.Value = (int)reader["Value"];
                test.FValue = (float)reader["FValue"];
                test.GValue = (double)reader["GValue"];
                test.Money = (decimal)reader["Money"];
                test.Timestamp = (DateTime)reader["Timestamp"];
                test.Duration = (TimeSpan)reader["Duration"];
                test.Guid = (Guid)reader["Guid"];
                reader.Close();
            }
            conn.Close();
        }
        private SqlSelect cachedSelect_test = new SqlSelect().From<test>().Select<test>();
        private ISqlExpression cachedSelect_test_simplified;
        private SqlSelect cachedSelect_testt = new SqlSelect().From<testt>().Select<testt>();
        private SqlSelect cachedSelect_test_inmemory = new SqlSelect().From<test>().Select<test>();
        private SqlSelect cachedSelect_testt_inmemory = new SqlSelect().From<testt>().Select<testt>();

        private ISqlExpression cachedSelect_testt_MSSQLSimplified;

        [Benchmark]
        public void DoDataToolsMSSQLSelectRecord()
        {
            var l = _mssqlcontext.Select<test>(new SqlSelect().From<test>().Select<test>().Where("i", c - 1)).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectManyRecords()
        {
            var l = _mssqlcontext.Select<test>(cachedSelect_test_simplified).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectManyRecordsWithReferences()
        {
            var l = _mssqlcontext.Select<testt>(cachedSelect_testt_MSSQLSimplified).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectRecordManyTimes()
        {
            var par = new SqlParameter("i");
            ISqlExpression cmd = new SqlSelect().From<test>().Select<test>().Where(new SqlWhere().Name("i").EqPar(par));
            cmd = new MSSQL_QueryParser().SimplifyQuery(cmd);
            for (int i = 0; i < c; ++i)
            {
                par.Value = i + 1;
                var l = _mssqlcontext.Select<test>(cmd, par).First();
            }
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectRecordDynamic()
        {
            var l = _mssqlcontext.Select(ModelMetadata<test>.Instance, new SqlSelect().From(ModelMetadata<test>.Instance).Select(ModelMetadata<test>.Instance).Where("i", c - 1)).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectManyRecordsDynamic()
        {
            var l = _mssqlcontext.Select(ModelMetadata<test>.Instance, cachedSelect_test).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectManyRecordsWithReferencesDynamic()
        {
            var l = _mssqlcontext.Select(ModelMetadata<testt>.Instance, cachedSelect_testt_MSSQLSimplified).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectRecordManyTimesDynamic()
        {
            var par = new SqlParameter("i");
            ISqlExpression cmd = new SqlSelect().From(ModelMetadata<test>.Instance).Select(ModelMetadata<test>.Instance).Where(new SqlWhere().Name("i").EqPar(par));
            cmd = new MSSQL_QueryParser().SimplifyQuery(cmd);
            var mm = ModelMetadata<test>.Instance;
            for (int i = 0; i < c; ++i)
            {
                par.Value = i + 1;
                var l = _mssqlcontext.Select(mm, cmd, par).First();
            }
        }

        [Benchmark]
        public void DoEntityFrameworkSelectRecord()
        {
            var context = new TestContext(connectionString);

            var l = context.test.Where(t => t.i == c - 1).ToList();
        }

        [Benchmark]
        public void DoEntityFrameworkSelectManyRecords()
        {
            var context = new TestContext(connectionString);

            var l = context.test.ToList();
        }

        [Benchmark]
        public void DoEntityFrameworkSelectManyRecordsWithReferences()
        {
            var context = new TesttContext(connectionString);

            //var l = context.testt.Include(t => t.test).AsNoTracking().ToList();
            var l = context.testt.Include(t => t.test).ToList();
        }

        [Benchmark]
        public void DoEntityFrameworkSelectRecordManyTimes()
        {
            var context = new TestContext(connectionString);
            for (int i = 0; i < c; ++i)
            {
                var l = context.test.Where(t => t.i == i + 1).First();
            }
        }

        [Benchmark]
        public void DoDapperSelectRecord()
        {
            var l = Dapper.SqlMapper.Query<test>(_connection, "select * from dbo.test where i = @i", new { i = c - 1 }).ToArray();
        }

        [Benchmark]
        public void DoDapperSelectManyRecords()
        {
            var l = Dapper.SqlMapper.Query<test>(_connection, "select * from dbo.test").ToArray();
        }
        [Benchmark]
        public void DoDapperSelectManyRecordsWithReferences()
        {
            var sql =
@"select * from dbo.testt tt
left join dbo.test t on tt.testi = t.i
";

            var data = Dapper.SqlMapper.Query<testt, test, testt>(_connection, sql, (tt, t) => { tt.test = t; return tt; }, splitOn: "i").ToArray();
        }
        [Benchmark]
        public void DoDapperSelectRecordManyTimes()
        {
            for (int i = 0; i < c; ++i)
            {
                var l = Dapper.SqlMapper.Query<test>(_connection, "select * from dbo.test where i = @i", new { i = i + 1 }).First();
            }
        }

        [Benchmark]
        public void DoDataToolsInMemorySelectRecord()
        {
            var l = _inmemorycontext.Select<test>(new SqlSelect().From<test>().Where("i", c - 1)).ToArray();
        }

        [Benchmark]
        public void DoDataToolsInMemorySelectManyRecords()
        {
            var l = _inmemorycontext.Select<test>(cachedSelect_test_inmemory).ToArray();
        }

        [Benchmark]
        public void DoDataToolsInMemorySelectRecordManyTimes()
        {
            var par = new DataTools.DML.SqlParameter("i");
            ISqlExpression cmd = new SqlSelect().From<test>().Where(new SqlWhere().Name("i").EqPar(par));
            cmd = new SQLite_QueryParser().SimplifyQuery(cmd);
            for (int i = 0; i < c; ++i)
            {
                par.Value = i + 1;
                var l = _inmemorycontext.Select<test>(cmd, par).First();
            }
        }

        [Benchmark]
        public void DoDataToolsInMemorySelectManyRecordsWithReferences()
        {
            var l = _inmemorycontext.Select<testt>(cachedSelect_testt_inmemory).ToArray();
        }

        private class TestContext : DbContext
        {
            public DbSet<test> test { get; set; }

            private string connectionString;

            public TestContext(string connectionString) : base()
            {
                this.connectionString = connectionString;
            }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                //base.OnModelCreating(modelBuilder);
                modelBuilder.Entity<test>().HasNoKey();
            }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                optionsBuilder.UseSqlServer(connectionString);
            }
        }

        private class TesttContext : DbContext
        {
            public DbSet<testt> testt { get; set; }
            public DbSet<test> test { get; set; }

            private string connectionString;

            public TesttContext(string connectionString) : base()
            {
                this.connectionString = connectionString;
            }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                //base.OnModelCreating(modelBuilder);
                modelBuilder.Entity<test>();
                modelBuilder.Entity<testt>();//.HasOne<test>();                
            }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                //optionsBuilder.EnableServiceProviderCaching(false);
                optionsBuilder.UseSqlServer(connectionString);
            }
        }
    }

    [ObjectName(nameof(test), "dbo")]
    internal class test
    {
        [DataTools.Attributes.Unique]
        [System.ComponentModel.DataAnnotations.Key]
        public int i { get; set; }

        public long? LongId { get; set; }
        public short? ShortId { get; set; }
        public string Name { get; set; }
        public string CharCode { get; set; }
        public bool? Checked { get; set; }
        public int? Value { get; set; }
        public float? FValue { get; set; }
        public double? GValue { get; set; }
        public decimal? Money { get; set; }
        public DateTime? Timestamp { get; set; }
        public TimeSpan? Duration { get; set; }
        public Guid? Guid { get; set; }
    }

    //[NoUnique]
    //[ObjectName(nameof(test_inmemory),"dbo")]
    //internal class test_inmemory
    //{
    //    public int i { get; set; }

    //    public long? LongId { get; set; }
    //    public short? ShortId { get; set; }
    //    public string Name { get; set; }
    //    public string CharCode { get; set; }
    //    public bool? Checked { get; set; }
    //    public int? Value { get; set; }
    //    public float? FValue { get; set; }
    //    public double? GValue { get; set; }
    //    public decimal? Money { get; set; }
    //    public DateTime? Timestamp { get; set; }
    //    public TimeSpan? Duration { get; set; }
    //    public Guid? Guid { get; set; }
    //}

    [ObjectName(nameof(testt), "dbo")]
    internal class testt
    {
        [DataTools.Attributes.Unique]
        [System.ComponentModel.DataAnnotations.Key]
        public int i { get; set; }

        public long? LongId { get; set; }
        public short? ShortId { get; set; }
        public string Name { get; set; }
        public string CharCode { get; set; }
        public bool? Checked { get; set; }
        public int? Value { get; set; }
        public float? FValue { get; set; }
        public double? GValue { get; set; }
        public decimal? Money { get; set; }
        public DateTime? Timestamp { get; set; }
        public TimeSpan? Duration { get; set; }
        public Guid? Guid { get; set; }

        [Reference(nameof(test.i), "testi")]
        public test? test { get; set; }
    }

    //[NoUnique]
    //[ObjectName(nameof(testt_inmemory), "dbo")]
    //internal class testt_inmemory
    //{
    //    public int i { get; set; }

    //    public long? LongId { get; set; }
    //    public short? ShortId { get; set; }
    //    public string Name { get; set; }
    //    public string CharCode { get; set; }
    //    public bool? Checked { get; set; }
    //    public int? Value { get; set; }
    //    public float? FValue { get; set; }
    //    public double? GValue { get; set; }
    //    public decimal? Money { get; set; }
    //    public DateTime? Timestamp { get; set; }
    //    public TimeSpan? Duration { get; set; }
    //    public Guid? Guid { get; set; }

    //    [Reference(nameof(test.i)), ColumnName("testi")]
    //    public test_inmemory? test { get; set; }
    //}
}
