using BenchmarkDotNet.Attributes;
using DataTools.Attributes;
using DataTools.Common;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.MSSQL;
using Microsoft.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using System.Text;

namespace DataTools_Benchmarks
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //BenchmarkDotNet.Running.BenchmarkRunner.Run<DataTools_Benchs>(BenchmarkDotNet.Configs.DefaultConfig.Instance.WithOption(ConfigOptions.DisableOptimizationsValidator, true));


            var sw = new Stopwatch();
            var b = new DataTools_Benchs();

            while (true)
            {
                Console.Clear();
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);


                sw.Restart();
                b.DoMSSQLSelectManyRecords();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoEntityFrameworkSelectManyRecords();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectManyRecords();
                Console.WriteLine($"DataTools {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectManyRecords)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                sw.Restart();
                b.DoMSSQLSelectRecordManyTimes();
                Console.WriteLine($"ADO NET {nameof(DataTools_Benchs.DoMSSQLSelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoEntityFrameworkSelectRecordManyTimes();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectRecordManyTimes)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectRecordManyTimes();
                Console.WriteLine($"DataTools {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectRecordManyTimes)} " + sw.Elapsed.ToString());
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
                b.DoEntityFrameworkSelectManyRecordsWithReferences();
                Console.WriteLine($"EF {nameof(DataTools_Benchs.DoEntityFrameworkSelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);
                sw.Restart();
                b.DoDataToolsMSSQLSelectManyRecordsWithReferences();
                Console.WriteLine($"DataTools {nameof(DataTools_Benchs.DoDataToolsMSSQLSelectManyRecordsWithReferences)} " + sw.Elapsed.ToString());
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true);

                Console.WriteLine("Again? Press Y...");
                if (Console.ReadKey().Key != ConsoleKey.Y) break;
            }

        }


    }

    [MemoryDiagnoser]
    public class DataTools_Benchs
    {
        [Params(10000)]
        public int c = 10000;
        private string connectionString = "Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=" + Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Database.mdf") + ";Integrated Security=True;Pooling=True";
        private MSSQL_DataContext _context;

        public DataTools_Benchs()
        {
            var context = DataManager.AddContext("mssql", new MSSQL_DataContext()) as MSSQL_DataContext;
            context.ConnectionString = connectionString;
            _context = context;

            var ds = _context.GetDataSource() as MSSQL_DataSource;
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
select i, 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59',newid(),1 + (i % 5) from t1
--select i, 1,1,'TestModel1','a',0,1,1.1,1.2,1.3,'2024-01-01','23:59:59',newid(),i from t1
option (maxrecursion 0);
");
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

        private SqlSelect cachedSelect = new SqlSelect().From<testt>();

        [Benchmark]
        public void DoDataToolsMSSQLSelectManyRecords()
        {
            var l = _context.Select<test>(cachedSelect).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectManyRecordsWithReferences()
        {
            var l = _context.Select<testt>(cachedSelect).ToArray();
        }

        [Benchmark]
        public void DoDataToolsMSSQLSelectRecordManyTimes()
        {
            var cmd = new SqlSelect().From<test>();
            for (int i = 1; i <= c; ++i)
            {
                cmd.Where("i", i);
                var l = _context.Select<test>(cmd).First();
            }
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

        [Reference(nameof(test.i)), ColumnName("testi")]
        public test? test { get; set; }
    }
}
