using DataTools.Common;
using DataTools.DML;
using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace DataTools.SQLite
{
    public sealed class SQLite_DataSource : DBMS_DataSource
    {
        private const int CACHE_SIZE = 64;
        private LinkedList<(string, ISqlExpression)> _plans = new LinkedList<(string, ISqlExpression)>();
        private Dictionary<string, LinkedListNode<(string, ISqlExpression)>> _queryCache = new Dictionary<string, LinkedListNode<(string, ISqlExpression)>>();
        private SqliteConnection _conn = new SqliteConnection();
        private SqliteCommand _command;

        public SqliteConnection Connection { get { return _conn; } }

        public SQLite_DataSource(string connectionString) : base(new SQLite_QueryParser())
        {
            _conn.ConnectionString = connectionString;
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
            _conn.Open();
            try
            {
                _command.CommandText = query;
                _command.ExecuteNonQuery();
            }
            finally { _conn.Close(); }
        }

        public object ExecuteScalar(string query)
        {
            _conn.Open();
            try
            {
                _command.CommandText = query;
                var result = _command.ExecuteScalar();
                return result == DBNull.Value ? null : result;
            }
            finally { _conn.Close(); }
        }

        public IEnumerable<object[]> ExecuteWithResult(string query)
        {
            SqliteDataReader reader = null;
            object[] array = null;
            object value = null;
            _conn.Open();
            try
            {
                _command.CommandText = query;
                reader = _command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess | System.Data.CommandBehavior.SingleResult);
                int fieldCount = reader.FieldCount;
                while (reader.Read())
                {
                    array = new object[fieldCount];
                    for (int i = 0; i < fieldCount; ++i)
                        array[i] = (value = reader[i]) == DBNull.Value ? null : value;
                    yield return array;
                }
            }
            finally
            {
                reader?.Close();
                _conn.Close();
            }
        }
    }
}