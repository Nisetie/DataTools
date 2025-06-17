using DataTools.DML;
using DataTools.Interfaces;
using System.Text;

namespace DataTools.MSSQL
{
    public sealed class MSSQL_QueryParser : DBMS_QueryParser
    {
        protected override string StringifyValue(object value)
        {
            return MSSQL_TypesMap.ToStringSQL(value);
        }

        protected override string Parse_SqlSelect(SqlSelect sqlSelect)
        {
            var sb = new StringBuilder(256);

            sb.Append("SELECT ");
            foreach (var s in sqlSelect.Selects)
            {
                sb.Append(ParseExpression(s));
                sb.Append(',');
            }
            sb.Length -= 1;
            sb.AppendLine();

            if (sqlSelect.FromSource == null) return sb.ToString(); // select без источника (select getdate())

            sb.AppendLine("FROM ");
            sb.Append(ParseExpression(sqlSelect.FromSource));
            sb.AppendLine();

            if (sqlSelect.Wheres != null)
            {
                sb.Append("WHERE ");
                sb.Append(ParseExpression(sqlSelect.Wheres));
                sb.AppendLine();
            }

            if (sqlSelect.Orders != null)
            {
                sb.Append("ORDER BY ");
                foreach (var o in sqlSelect.Orders)
                {
                    sb.Append(ParseExpression(o));
                    sb.Append(',');
                }
                sb.Length -= 1;
                sb.AppendLine();
            }

            if (sqlSelect.OffsetRows != null)
            {
                sb.Append("OFFSET ");
                sb.Append(ParseExpression(sqlSelect.OffsetRows));
                sb.AppendLine(" rows ");
                if (sqlSelect.LimitRows != null)
                {
                    sb.AppendLine("fetch next ");
                    sb.Append(ParseExpression(sqlSelect.LimitRows));
                    sb.AppendLine(" rows only");
                }
            }

            return sb.ToString();
        }

        protected override string Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias)
        {
            if (sqlExpressionWithAlias.SqlExpression is SqlFunction)
                return $"{ParseExpression(sqlExpressionWithAlias.SqlExpression)} as {sqlExpressionWithAlias.Alias}";
            else
                return $"({ParseExpression(sqlExpressionWithAlias.SqlExpression)}) as {sqlExpressionWithAlias.Alias}";
        }

        protected override string Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause)
        {
            return $"{ParseExpression(sqlOrderByClause.OrderValue)} {sqlOrderByClause.Order}";
        }

        protected override string Parse_SqlDelete(SqlDelete sqlDelete)
        {
            var sb = new StringBuilder(128)
                .Append("DELETE FROM ")
                .Append(ParseExpression(sqlDelete.FromSource))
                .AppendLine();
            if (sqlDelete.Wheres != null)
            {
                sb.Append("WHERE ")
                .Append(ParseExpression(sqlDelete.Wheres));
            }
            return sb.ToString();
        }

        protected override string Parse_SqlInsert(SqlInsert sqlInsert)
        {
            var sb = new StringBuilder(256);
            sb
                .Append("INSERT INTO ")
                .Append(ParseExpression(sqlInsert.IntoDestination))
                .Append("(");
            foreach (var c in sqlInsert.Columns)
                sb.Append(ParseExpression(c)).Append(",");
            sb
                .Remove(sb.Length - 1, 1)
                .AppendLine(")")
                .AppendLine("output inserted.*")
                .AppendLine("values")
                .Append("(");
            foreach (var v in sqlInsert.Values)
                sb.Append(ParseExpression(v)).Append(",");
            sb
                .Remove(sb.Length - 1, 1)
                .AppendLine("),")
                .Remove(sb.Length - 3, 3)
                .AppendLine();
            return sb.ToString();
        }

        protected override string Parse_SqlUpdate(SqlUpdate sqlUpdate)
        {
            var sb = new StringBuilder(256);
            sb
                .Append("UPDATE ")
                .Append(ParseExpression(sqlUpdate.FromSource))
                .AppendLine()
                .AppendLine("SET");

            var columnsE = sqlUpdate.Columns.GetEnumerator();
            var valuesE = sqlUpdate.Values.GetEnumerator();
            while (columnsE.MoveNext() && valuesE.MoveNext())
            {
                sb
                    .Append(ParseExpression(columnsE.Current))
                    .Append("=")
                    .Append(ParseExpression(valuesE.Current))
                    .AppendLine(",");
            }
            sb
                .Remove(sb.Length - 3, 3)
                .AppendLine()
                .AppendLine("output inserted.*");
            if (sqlUpdate.Wheres != null)
            {
                sb.Append("WHERE ")
                .Append(ParseExpression(sqlUpdate.Wheres))
                .AppendLine();
            }
            return sb.ToString();
        }

        protected override string Parse_SqlFunction(SqlFunction sqlFunction)
        {
            var sb = new StringBuilder();
            sb.Append($"{sqlFunction.FunctionName}( ");
            foreach (var p in sqlFunction.Parameters)
                sb.Append(ParseExpression(p)).Append(",");
            return sb
                .Remove(sb.Length - 1, 1)
                .Append(")")
                .ToString();
        }

        protected override string Parse_SqlProcedure(SqlProcedure sqlProcedure)
        {
            var sb = new StringBuilder();
            sb.Append($"EXEC {sqlProcedure.ProcedureName} ");
            foreach (var p in sqlProcedure.Parameters)
                sb.Append(ParseExpression(p)).Append(",");
            return sb
                .Remove(sb.Length - 1, 1)
                .Append(";")
                .ToString();
        }
    }
}

