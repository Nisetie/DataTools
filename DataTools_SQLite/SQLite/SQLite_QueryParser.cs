using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Text;

namespace DataTools.SQLite
{
    public class SQLite_QueryParser : DBMS_QueryParser
    {

        protected override string StringifyValue(object value)
        {
            return SQLite_TypesMap.ToStringSQL(value);
        }

        protected override string Parse_SqlSelect(SqlSelect sqlSelect)
        {
            var sb = new StringBuilder(256);
            sb.Append("SELECT ");

            foreach (var s in sqlSelect.Selects)
                sb
                    .Append(ParseExpression(s))
                    .Append(',');
            sb.Length -= 1;
            sb.AppendLine();

            if (sqlSelect.FromSource == null) return sb.ToString(); // select без источника (select getdate())

            sb.AppendLine("FROM ");

            var name = sqlSelect.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            sb.Append(name).AppendLine();

            if (sqlSelect.Wheres != null)
                sb
                    .Append("WHERE ")
                    .Append(ParseExpression(sqlSelect.Wheres))
                    .AppendLine();

            if (sqlSelect.Orders != null)
            {
                sb.Append("ORDER BY ");

                foreach (var o in sqlSelect.Orders)
                    sb
                        .Append(ParseExpression(o))
                        .Append(',');
                sb.Length -= 1;
                sb.AppendLine();
            }

            if (sqlSelect.OffsetRows != null)
            {
                sb
                    .Append("limit ")
                    .Append(ParseExpression(sqlSelect.OffsetRows))
                    .AppendLine(",");
                if (sqlSelect.LimitRows != null)
                    sb.Append(ParseExpression(sqlSelect.LimitRows));
            }
            return sb.ToString();
        }
        protected override string Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias)
        {
            return $"({ParseExpression(sqlExpressionWithAlias.SqlExpression)}) as {sqlExpressionWithAlias.Alias}";
        }

        protected override string Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause)
        {
            return $"{ParseExpression(sqlOrderByClause.OrderValue)} {sqlOrderByClause.Order}";
        }

        protected override string Parse_SqlDelete(SqlDelete sqlDelete)
        {
            var sb = new StringBuilder(128);
            sb.Append("DELETE FROM ");

            var name = sqlDelete.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            sb.Append(name).AppendLine();
            if (sqlDelete.Wheres != null)
                sb.Append("WHERE ").Append(ParseExpression(sqlDelete.Wheres));
            return sb.ToString();
        }

        protected override string Parse_SqlInsert(SqlInsert sqlInsert)
        {
            var sb = new StringBuilder(256);
            sb.Append("INSERT INTO ");

            var name = sqlInsert.IntoDestination.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            sb.Append(name).Append("(");

            foreach (var c in sqlInsert.Columns)
                sb.Append(ParseExpression(c)).Append(",");
            sb
                .Remove(sb.Length - 1, 1)
                .AppendLine(")")
                .AppendLine("values")
                .Append("(");
            foreach (var v in sqlInsert.Values)
                sb.Append(ParseExpression(v)).Append(",");
            return sb
                .Remove(sb.Length - 1, 1)
                .AppendLine("),")
                .Remove(sb.Length - 3, 3)
                .AppendLine()
                .AppendLine("returning *;")
                .ToString();
        }

        protected override string Parse_SqlUpdate(SqlUpdate sqlUpdate)
        {
            var sb = new StringBuilder(256);
            sb.Append("UPDATE ");

            var name = sqlUpdate.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            sb
                .Append(name)
                .AppendLine()
                .AppendLine("SET");


            var columnsE = sqlUpdate.Columns.GetEnumerator();
            var valuesE = sqlUpdate.Values.GetEnumerator();
            while (columnsE.MoveNext() && valuesE.MoveNext())
                sb
                    .Append(ParseExpression(columnsE.Current))
                    .Append("=")
                    .Append(ParseExpression(valuesE.Current))
                    .AppendLine(",");
            sb
                .Remove(sb.Length - 3, 3)
                .AppendLine();

            if (sqlUpdate.Wheres != null)
            {
                sb.Append("WHERE ")
                    .Append(ParseExpression(sqlUpdate.Wheres))
                    .AppendLine();
            }
            sb.AppendLine("returning *;");
            return sb.ToString();
        }

        protected override string Parse_SqlFunction(SqlFunction sqlFunction)
        {
            var sb = new StringBuilder();
            sb.Append($"{sqlFunction.FunctionName}( ");
            foreach (var p in sqlFunction.Parameters)
                sb.Append(ParseExpression(p)).Append(",");
            return sb.Remove(sb.Length - 1, 1).Append(")").ToString();
        }

        protected override string Parse_SqlProcedure(SqlProcedure sqlProcedure)
        {
            throw new NotImplementedException();
        }
    }
}