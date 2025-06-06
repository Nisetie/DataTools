using DataTools.DML;
using DataTools.Interfaces;
using System;

namespace DataTools.SQLite
{
    public class SQLite_QueryParser : DBMS_QueryParser
    {

        protected override string StringifyValue(object value)
        {
            return SQLite_TypesMap.ToStringSQL(value);
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

            var name = sqlSelect.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _query.Append(name).AppendLine();

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
                _query.Append("limit ");
                ParseExpression(sqlSelect.OffsetRows);
                _query.AppendLine(",");
                if (sqlSelect.LimitRows != null)
                    ParseExpression(sqlSelect.LimitRows);
            }
        }
        protected override void Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias)
        {
            _query.Append("(");
            ParseExpression(sqlExpressionWithAlias.SqlExpression);
            _query.AppendLine($") as {sqlExpressionWithAlias.Alias}");
        }

        protected override void Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause)
        {
            ParseExpression(sqlOrderByClause.OrderValue);
            _query.Append(" ").Append(sqlOrderByClause.Order);
        }

        protected override void Parse_SqlWhereClause(SqlWhereClause sqlWhereClause)
        {
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
            _query.Append($"{sqlName}");
        }

        protected override void Parse_SqlDelete(SqlDelete sqlDelete)
        {
            _query.Append("DELETE FROM ");

            var name = sqlDelete.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _query
                .Append(name)
                .AppendLine()
                .Append("WHERE ");
            ParseExpression(sqlDelete.Wheres);
        }

        protected override void Parse_SqlInsert(SqlInsert sqlInsert)
        {
            _query.Append("INSERT INTO ");

            var name = sqlInsert.IntoDestination.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _query.Append(name).Append("(");

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
                .AppendLine("returning *;");
        }

        protected override void Parse_SqlUpdate(SqlUpdate sqlUpdate)
        {
            _query.Append("UPDATE ");

            var name = sqlUpdate.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _query
                .Append(name)
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
                .AppendLine("returning *;");
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
            throw new NotImplementedException();
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
        protected override void Parse_SqlComposition(SqlComposition sqlComposition)
        {
            foreach (var el in sqlComposition.Elements)
            {
                if (el is SqlExpression) ParseExpression((SqlExpression)el);
                else _query.Append(el.ToString());
            }
        }

        protected override void Parse_SqlParameter(SqlParameter sqlParameter)
        {
            string parName = sqlParameter.Name;
            for (var i = 0; i < _currentParams.Length; i++)
                if (parName == _currentParams[i].Name)
                {
                    var p = _currentParams[i].Value;
                    if (p is SqlExpression sqlExpression) ParseExpression(sqlExpression);
                    else _query.Append($"{StringifyValue(p)}");
                    return;
                }
            _query.Append($"{StringifyValue(null)}");
        }
    }
}