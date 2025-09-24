using DataTools.Common;
using DataTools.DML;
using DataTools.SQLite;
using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace DataTools.InMemory_SQLite
{
    public sealed class InMemory_SQLite_DataSource : DBMS_DataSource, IDisposable
    {
        private const int CACHE_SIZE = 64;
        private LinkedList<(string, ISqlExpression)> _plans = new LinkedList<(string, ISqlExpression)>();
        private Dictionary<string, LinkedListNode<(string, ISqlExpression)>> _queryCache = new Dictionary<string, LinkedListNode<(string, ISqlExpression)>>();
        private SqliteConnection _conn = new SqliteConnection();
        private SqliteCommand _command;

        public SqliteConnection Connection { get { return _conn; } }

        public InMemory_SQLite_DataSource(string connectionString) : base(new SQLite_QueryParser())
        {
            _conn.ConnectionString = connectionString;
            _conn.Open();
            _command = _conn.CreateCommand();
        }
        private ISqlExpression GetFromCache(ISqlExpression query)
        {
            string queryString = query.ToString();
            if (!_queryCache.TryGetValue(queryString, out var node))
            {
                _queryCache[queryString] = node = _plans.AddFirst((queryString, _queryParser.SimplifyQuery(query)));
                if (_plans.Count > CACHE_SIZE)
                {
                    _queryCache.Remove(_plans.Last.Value.Item1);
                    _plans.RemoveLast();
                }
            }
            else if (node.Previous != null)
            {
                var prev = node.Previous;
                _plans.Remove(node);
                _plans.AddBefore(prev, node);
            }
            return node.Value.Item2;
        }

        public override void Execute(ISqlExpression query, params DML.SqlParameter[] parameters)
        {
            Execute(_queryParser.ToString(GetFromCache(query), parameters));
        }
        public override object ExecuteScalar(ISqlExpression query, params DML.SqlParameter[] parameters)
        {
            return ExecuteScalar(_queryParser.ToString(GetFromCache(query), parameters));
        }
        public override IEnumerable<object[]> ExecuteWithResult(ISqlExpression query, params DML.SqlParameter[] parameters)
        {
            return ExecuteWithResult(_queryParser.ToString(GetFromCache(query), parameters));
        }

        public void Execute(string query)
        {
            _command.CommandText = query;
            _command.ExecuteNonQuery();
        }

        public object ExecuteScalar(string query)
        {
            _command.CommandText = query;
            var result = _command.ExecuteScalar();
            return result == DBNull.Value ? null : result;
        }

        public IEnumerable<object[]> ExecuteWithResult(string query)
        {
            SqliteDataReader reader = null;
            object v;
            try
            {
                _command.CommandText = query;
                reader = _command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess | System.Data.CommandBehavior.SingleResult);
                if (reader.HasRows)
                {
                    int fieldCount = reader.FieldCount;
                    var array = new object[fieldCount];
                    while (reader.Read())
                    {
                        for (int i = 0; i < fieldCount; ++i)
                            array[i] = (v = reader[i]) == DBNull.Value ? null : v;
                        yield return array;
                    }
                }

            }
            finally { reader?.Close(); }
        }

        public void Dispose()
        {
            _command.Dispose();
            _command = null;
            _conn.Close();
            _conn.Dispose();
            _conn = null;
        }
    }
}