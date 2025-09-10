using DataTools.Common;
using DataTools.DML;
using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace DataTools.SQLite
{
    public sealed class SQLite_DataSource : DBMS_DataSource
    {
        private SqliteConnection _conn = new SqliteConnection();
        private SqliteCommand _command;

        public SqliteConnection Connection { get { return _conn; } }

        public SQLite_DataSource(string connectionString) : base(new SQLite_QueryParser())
        {
            _conn.ConnectionString = connectionString;
            _command = _conn.CreateCommand();
        }
        public override void Execute(SqlExpression query, params SqlParameter[] parameters)
        {
            Execute(_queryParser.ToString(query, parameters));
        }
        public override object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters)
        {
            return ExecuteScalar(_queryParser.ToString(query, parameters));
        }
        public override IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters)
        {
            return ExecuteWithResult(_queryParser.ToString(query, parameters));
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