using DataTools.Common;
using DataTools.DML;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_DataSource : DBMS_DataSource
    {
        private NpgsqlConnection _conn = new NpgsqlConnection();
        private NpgsqlCommand _command;

        public PostgreSQL_DataSource(string connectionString) : base(new PostgreSQL_QueryParser())
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
            return result;
        }

        /// <summary>
        /// Получить итератор для обхода строк результата запроса.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public IEnumerable<object[]> ExecuteWithResult(string query)
        {
            NpgsqlDataReader r = null;
            object v;
            _conn.Open();
            try
            {
                _command.CommandText = query;
                r = _command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
                if (r.HasRows)
                {
                    int fieldCount = r.FieldCount;
                    var array = new object[fieldCount];
                    while (r.Read())
                    {
                        for (int i = 0; i < fieldCount; ++i)
                            array[i] = (v = r[i]) == DBNull.Value ? null : v;
                        yield return array;
                    }
                }
            }
            finally
            {
                r.Close();
                _conn.Close();
            }
        }
    }
}