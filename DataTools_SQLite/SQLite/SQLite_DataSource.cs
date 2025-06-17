using DataTools.Common;
using DataTools.DML;
using System;
using System.Collections.Generic;
using System.Data.SQLite;

namespace DataTools.SQLite
{
    public sealed class SQLite_DataSource : DBMS_DataSource
    {
        private SQLiteConnection _conn = new SQLiteConnection();
        private SQLiteCommand _command;

        public SQLiteConnection Connection { get { return _conn; } }

        public SQLite_DataSource(string connectionString) : base(new SQLite_QueryParser())
        {
            _conn.ConnectionString = connectionString;
            _command = _conn.CreateCommand();
        }

        public override void Execute(SqlExpression query) => Execute(_queryParser.ToString(query));
        public override void Execute(SqlExpression query, params SqlParameter[] parameters)
        {
            Execute(_queryParser.ToString(query, parameters));
        }
        public override object ExecuteScalar(SqlExpression query) => ExecuteScalar(_queryParser.ToString(query));
        public override object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters)
        {
            return ExecuteScalar(_queryParser.ToString(query, parameters));
        }
        public override IEnumerable<object[]> ExecuteWithResult(SqlExpression query) => ExecuteWithResult(_queryParser.ToString(query));
        public override IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters)
        {
            return ExecuteWithResult(_queryParser.ToString(query, parameters));
        }

        public void Execute(string query)
        {
            _conn.Open();
            _command.CommandText = query;
            _command.ExecuteNonQuery();
            _conn.Close();
        }

        public object ExecuteScalar(string query)
        {
            _conn.Open();
            _command.CommandText = query;
            var result = _command.ExecuteScalar();
            _conn.Close();
            return result == DBNull.Value ? null : result;
        }

        public IEnumerable<object[]> ExecuteWithResult(string query)
        {
            SQLiteDataReader reader = null;
            object v;
            _conn.Open();
            try
            {
                _command.CommandText = query;
                reader = _command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess | System.Data.CommandBehavior.SingleResult);
                int fieldsCount = reader.FieldCount;
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
            finally { reader?.Close(); _conn.Close(); }
        }
    }
}