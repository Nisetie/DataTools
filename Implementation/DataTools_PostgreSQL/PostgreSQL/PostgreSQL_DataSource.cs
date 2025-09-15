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

        static PostgreSQL_DataSource()
        {
            // чтобы timestamptz автоматически переводился в локальное время
            //AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
        }

        public PostgreSQL_DataSource(string connectionString) : base(new PostgreSQL_QueryParser())
        {
            _conn.ConnectionString = connectionString;
            _command = _conn.CreateCommand();
        }
        public override void Execute(ISqlExpression query, params SqlParameter[] parameters)
        {
            Execute(_queryParser.ToString(query, parameters));
        }
        public override object ExecuteScalar(ISqlExpression query, params SqlParameter[] parameters)
        {
            return ExecuteScalar(_queryParser.ToString(query, parameters));
        }
        public override IEnumerable<object[]> ExecuteWithResult(ISqlExpression query, params SqlParameter[] parameters)
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
            finally
            {
                _conn.Close();
            }
        }

        public object ExecuteScalar(string query)
        {
            _conn.Open();
            try
            {
                _command.CommandText = query;
                var result = _command.ExecuteScalar();

                return result;
            }
            finally { _conn.Close(); }
        }

        /// <summary>
        /// Получить итератор для обхода строк результата запроса.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public IEnumerable<object[]> ExecuteWithResult(string query)
        {
            NpgsqlDataReader reader = null;
            object[] array = null;
            object value = null;
            _conn.Open();
            try
            {
                _command.CommandText = query;
                reader = _command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
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