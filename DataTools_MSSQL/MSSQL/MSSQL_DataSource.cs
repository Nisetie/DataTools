using DataTools.Common;
using DataTools.DML;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace DataTools.MSSQL
{
    public sealed class MSSQL_DataSource : DBMS_DataSource
    {
        private SqlConnection _conn = new SqlConnection();
        private SqlCommand _command;

        public MSSQL_DataSource(string connectionString) : base(new MSSQL_QueryParser())
        {
            _conn.ConnectionString = connectionString;
            _command = _conn.CreateCommand();
        }
        public override void Execute(SqlExpression query, params DML.SqlParameter[] parameters)
        {
            Execute(_queryParser.ToString(query, parameters));
        }
        public override object ExecuteScalar(SqlExpression query, params DML.SqlParameter[] parameters)
        {
            return ExecuteScalar(_queryParser.ToString(query, parameters));
        }
        public override IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params DML.SqlParameter[] parameters)
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
                return result;
            }
            finally { _conn.Close(); }
        }

        public IEnumerable<object[]> ExecuteWithResult(string query)
        {
            SqlDataReader reader = null;
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

