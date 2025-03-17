using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_DataSource : DataSource
    {
        private NpgsqlConnection _conn = new NpgsqlConnection();
        private StringBuilder _query = new StringBuilder();

        public PostgreSQL_DataSource() : base() { }

        public override void Initialize(IDataContext dataContext)
        {
            base.Initialize(dataContext);
            if (!(dataContext is PostgreSQL_DataContext context)) return;
            _conn.ConnectionString = context.ConnectionString;
        }

        public void Execute(string query)
        {
            _conn.Open();
            new NpgsqlCommand(query, _conn).ExecuteNonQuery();
            _conn.Close();
        }

        public object ExecuteScalar(string query)
        {
            _conn.Open();
            var result = new NpgsqlCommand(query, _conn).ExecuteScalar();
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
            NpgsqlDataReader reader = null;
            _conn.Open();
            try
            {
                reader = new NpgsqlCommand(query, _conn).ExecuteReader(CommandBehavior.SequentialAccess);
                if (reader.HasRows)
                {
                    var array = new object[reader.FieldCount];
                    while (reader.Read())
                    {
                        for (int i = 0; i < array.Length; ++i)
                        {
                            var v = reader[i];
                            array[i] = (v == DBNull.Value ? null : v);
                        }
                        yield return array;
                    }
                }
            }
            finally { reader.Close(); _conn.Close(); }
        }

        protected override void _BeforeParsing()
        {
            _query.Clear();
        }

        protected override void _ExecuteAfterParsing()
        {
            this.Execute(_query.ToString());
        }

        public string GetSqlQuery(SqlExpression expression)
        {
            _BeforeParsing();
            _Parse(expression);
            return _query.ToString();
        }

        protected override string StringifyValue(object value)
        {
            return PostgreSQL_TypesMap.ToStringSQL(value);
        }

        protected override object _ExecuteScalarAfterParsing()
        {
            return this.ExecuteScalar(_query.ToString());
        }

        protected override IEnumerable<object[]> _ExecuteWithResultAfterParsing()
        {
            return this.ExecuteWithResult(_query.ToString());
        }

        protected override void Parse_SqlSelect(SqlSelect sqlSelect)
        {
            _query.Append("SELECT ");

            foreach (var s in sqlSelect.Selects)
            {
                ParseExpression(s);
                _query.Append(',');
            }
            _query.AppendLine("(select 1) as fakeOrder");

            if (sqlSelect.FromSource == null) return; // select без источника (select getdate())

            _query.AppendLine("FROM ");
            ParseExpression(sqlSelect.FromSource);
            _query.AppendLine();

            if (sqlSelect.Wheres != null)
            {
                _query.Append("WHERE ");
                ParseExpression(sqlSelect.Wheres);
                _query.AppendLine();
            }

            _query.Append("ORDER BY ");
            if (sqlSelect.Orders != null)
                foreach (var o in sqlSelect.Orders)
                {
                    ParseExpression(o);
                    _query.Append(',');
                }
            _query.AppendLine("fakeOrder");

            if (sqlSelect.OffsetRows != null)
            {
                _query.Append("OFFSET ");
                ParseExpression(sqlSelect.OffsetRows);
                _query.AppendLine(" rows ");
                if (sqlSelect.LimitRows != null)
                {
                    _query.AppendLine("fetch next ");
                    ParseExpression(sqlSelect.LimitRows);
                    _query.AppendLine(" rows only");
                }
            }
        }

        protected override void Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias)
        {
            if (sqlExpressionWithAlias.SqlExpression is SqlFunction)
            {
                ParseExpression(sqlExpressionWithAlias.SqlExpression);
                _query.AppendLine($" as {sqlExpressionWithAlias.Alias}");
            }
            else
            {
                _query.Append("(");
                ParseExpression(sqlExpressionWithAlias.SqlExpression);
                _query.AppendLine($") as {sqlExpressionWithAlias.Alias}");
            }
        }

        protected override void Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause)
        {
            ParseExpression(sqlOrderByClause.OrderValue);
            _query.Append(" ").Append(sqlOrderByClause.Order);
        }

        protected override void Parse_SqlWhereClause(SqlWhereClause sqlWhereClause)
        {

            //if (sqlWhereClause.Right != null)
            //{
            //    _query.Append("(");
            //    ParseExpression(sqlWhereClause.Left);
            //    _query.Append(" ");
            //    switch (sqlWhereClause.Operator)
            //    {
            //        case E_OP.EQ: _query.Append("="); break;
            //        case E_OP.NE: _query.Append("<>"); break;
            //        case E_OP.GT: _query.Append(">"); break;
            //        case E_OP.GE: _query.Append(">="); break;
            //        case E_OP.LT: _query.Append("<"); break;
            //        case E_OP.LE: _query.Append("<="); break;
            //        case E_OP.AND: _query.Append("AND"); break;
            //        case E_OP.OR: _query.Append("OR"); break;
            //    };
            //    _query.Append(" ");
            //    ParseExpression(sqlWhereClause.Right);
            //    _query.Append(")");
            //}
            //else
            //{
            //    _query.Append("(");
            //    ParseExpression(sqlWhereClause.Left);
            //    _query.Append(")");
            //}

            _query.Append("(");
            foreach (var el in sqlWhereClause.Nodes)
                ParseExpression(el);
            _query.Append(")");

        }
        protected override void Parse_SqlConstant(SqlConstant sqlConstant)
        {
            if (sqlConstant.Value is SqlExpression expression)
                ParseExpression(expression);
            else
                _query.Append(StringifyValue(sqlConstant.Value));
        }

        protected override void Parse_SqlCustom(SqlCustom sqlCustomQuery)
        {
            _query.Append(sqlCustomQuery.Query);
        }

        protected override void Parse_SqlName(SqlName sqlName)
        {
            _query.Append($"{sqlName.Name}");
        }

        protected override void Parse_SqlDelete(SqlDelete sqlDelete)
        {
            _query.Append("DELETE FROM ");
            ParseExpression(sqlDelete.FromSource);
            _query
                .AppendLine()
                .Append("WHERE ");
            ParseExpression(sqlDelete.Wheres);
        }

        protected override void Parse_SqlInsert(SqlInsert sqlInsert)
        {
            _query.Append("INSERT INTO ");
            ParseExpression(sqlInsert.IntoDestination);
            _query.Append("(");
            foreach (var c in sqlInsert.Columns)
            {
                ParseExpression(c);
                _query.Append(",");
            }
            _query
                .Remove(_query.Length - 1, 1)
                .AppendLine(")")
                .AppendLine("values")
                .Append("(");
            foreach (var v in sqlInsert.Values)
            {
                ParseExpression(v);
                _query.Append(",");
            }
            _query
                .Remove(_query.Length - 1, 1)
                .AppendLine("),")
                .Remove(_query.Length - 3, 3)
                .AppendLine()
                .AppendLine("returning *");
        }

        protected override void Parse_SqlUpdate(SqlUpdate sqlUpdate)
        {
            _query.Append("UPDATE ");
            ParseExpression(sqlUpdate.FromSource);
            _query
                .AppendLine()
                .AppendLine("SET");

            int i = 0;
            var columnsE = sqlUpdate.Columns.GetEnumerator();
            var valuesE = sqlUpdate.Values.GetEnumerator();
            while (columnsE.MoveNext() && valuesE.MoveNext())
            {
                ParseExpression(columnsE.Current);
                _query.Append("=");
                ParseExpression(valuesE.Current);
                _query.AppendLine(",");
                i++;
            }
            _query
                .Remove(_query.Length - 3, 3)
                .AppendLine();

            _query.Append("WHERE ");
            ParseExpression(sqlUpdate.Wheres);
            _query
                .AppendLine()
                .AppendLine("returning *");
        }

        protected override void Parse_SqlFunction(SqlFunction sqlFunction)
        {
            _query.Append($"{sqlFunction.FunctionName}( ");

            foreach (var p in sqlFunction.Parameters)
            {
                ParseExpression(p);
                _query.Append(",");
            }
            _query.Remove(_query.Length - 1, 1).Append(")");
        }

        protected override void Parse_SqlProcedure(SqlProcedure sqlProcedure)
        {
            _query.Append($"CALL {sqlProcedure.ProcedureName}( ");

            foreach (var p in sqlProcedure.Parameters)
            {
                ParseExpression(p);
                _query.Append(",");
            }
            _query.Remove(_query.Length - 1, 1).Append(");");
        }

        protected override void Parse_SqlOr(SqlOr sqlOr)
        {
            _query.Append(" OR ");
        }

        protected override void Parse_SqlAnd(SqlAnd sqlAnd)
        {
            _query.Append(" AND ");
        }

        protected override void Parse_SqlIsNull(SqlIsNull sqlIsNull)
        {
            _query.Append(" IS NULL ");
        }

        protected override void Parse_SqlEqual(SqlEqual sqlEqual)
        {
            _query.Append(" = ");
        }

        protected override void Parse_SqlLesserOrEqual(SqlLessOrEqual sqlLesserOrEqual)
        {
            _query.Append(" <= ");
        }

        protected override void Parse_SqlLesserThan(SqlLessThan sqlLesserThan)
        {
            _query.Append(" < ");
        }

        protected override void Parse_SqlGreaterOrEqual(SqlGreaterOrEqual sqlGreaterOrEqual)
        {
            _query.Append(" >= ");
        }

        protected override void Parse_SqlGreaterThan(SqlGreaterThan sqlGreaterThan)
        {
            _query.Append(" > ");
        }

        protected override void Parse_SqlNot(SqlNot sqlNot)
        {
            _query.Append(" NOT ");
        }

        protected override void Parse_SqlNotEqual(SqlNotEqual sqlNotEqual)
        {
            _query.Append(" <> ");
        }
    }
}