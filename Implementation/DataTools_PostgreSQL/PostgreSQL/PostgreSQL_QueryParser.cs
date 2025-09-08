using DataTools.Common;
using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Text;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_QueryParser : DBMS_QueryParser
    {
        static PostgreSQL_QueryParser _instance;
        public static PostgreSQL_QueryParser Instance => _instance;
        static PostgreSQL_QueryParser()
        {
        
        }

        protected override string StringifyValue(object value)
        {
            return PostgreSQL_TypesMapper.ToStringSQL(value);
        }

        protected override string Parse_SqlSelect(SqlSelect sqlSelect)
        {
            var sb = new StringBuilder(256);
            sb.Append("SELECT ");
            if (sqlSelect.Selects == null)
                sb.Append("*");
            else
            {
                foreach (var s in sqlSelect.Selects)
                    sb.Append(ParseExpression(s)).Append(',');
                sb.Length -= 1;
            }
            sb.AppendLine();

            if (sqlSelect.FromSource == null) return sb.ToString(); // select без источника (select getdate())

            sb.AppendLine("FROM ");
            if (sqlSelect.FromSource is SqlSelect sqlSelect1)
            {
                sb
                    .Append("(")
                    .Append(Parse_SqlSelect(sqlSelect1))
                    .Append(")");
            }
            else sb.Append(ParseExpression(sqlSelect.FromSource));
            sb.AppendLine();

            if (sqlSelect.Wheres != null)
                sb
                    .Append("WHERE ")
                    .Append(ParseExpression(sqlSelect.Wheres))
                    .AppendLine();

            if (sqlSelect.Orders != null)
            {
                sb.Append("ORDER BY ");
                foreach (var o in sqlSelect.Orders)
                    sb.Append(ParseExpression(o)).Append(',');
                sb.Length -= 1;
                sb.AppendLine();
            }

            if (sqlSelect.OffsetRows != null)
            {
                sb
                    .Append("OFFSET (")
                    .Append(ParseExpression(sqlSelect.OffsetRows))
                    .AppendLine(") rows ");
                if (sqlSelect.LimitRows != null)
                    sb
                        .AppendLine("fetch next (")
                        .Append(ParseExpression(sqlSelect.LimitRows))
                        .AppendLine(") rows only");
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
            sb.AppendLine(";");
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

        protected override string Parse_SqlInsertBatch(SqlInsertBatch sqlInsertBatch)
        {
            var sb = new StringBuilder(256);
            sb
                .Append("INSERT INTO ")
                .Append(ParseExpression(sqlInsertBatch.IntoDestination))
                .Append("(");
            foreach (var c in sqlInsertBatch.Columns)
                sb.Append(ParseExpression(c)).Append(",");
            sb
                .Remove(sb.Length - 1, 1)
                .AppendLine(")")
                .AppendLine("values");

            foreach (var valuesRow in sqlInsertBatch.Values)
            {
                sb.Append("(");
                foreach (var value in valuesRow)
                    sb.Append(ParseExpression(value)).Append(",");
                sb.Length -= 1;
                sb.AppendLine("),");
            }
            sb.Length -= 3;
            return sb
                .AppendLine()
                .AppendLine("returning *;")
                .ToString();
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
            return sb.AppendLine(";").ToString();
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
            sb.Append($"CALL {sqlProcedure.ProcedureName}( ");

            foreach (var p in sqlProcedure.Parameters)
                sb.Append(ParseExpression(p)).Append(",");
            return sb
                .Remove(sb.Length - 1, 1)
                .Append(");")
                .ToString();
        }

        protected override string Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement)
        {
            return "GENERATED ALWAYS AS IDENTITY";
        }

        protected override string Parse_SqlDropTable(SqlDropTable sqlDropTable)
        {
            var name = sqlDropTable.TableName.Name;
            return $"DROP TABLE IF EXISTS {name};";
        }

        protected override string Parse_SqlName(SqlName sqlName)
        {
            string name = sqlName.Name.ToLower();
            if (name.Contains(" "))
                return $"\"{name}\"";
            else return name;
        }

        protected override string Parse_SqlCreateTable(SqlCreateTable sqlCreateTable)
        {
            var sb = new StringBuilder(256);

            var name = sqlCreateTable.TableName.Name;

            sb.AppendLine($"CREATE TABLE IF NOT EXISTS {sqlCreateTable.TableName} (");

            foreach (var column in sqlCreateTable.Columns)
            {
                sb.Append($"{Parse_SqlName(column.ColumnName)} ");

                var colType = column.ColumnType;
                var sqlType = PostgreSQL_TypesMapper.GetSqlType(colType);
                if (sqlType == null) throw new NullReferenceException($"{nameof(PostgreSQL_QueryParser)}.{nameof(Parse_SqlCreateTable)}: {name}.{column.ColumnName} {colType}");

                if (colType.HasLength && sqlType != "text" && sqlType != "bytea")
                {
                    var textLength = column.TextLength;
                    string textLengthString = string.Empty;
                    if (textLength != null)
                    {
                        if (textLength > 0)
                            textLengthString = $"({textLength})";
                    }
                    sb.Append($"{sqlType}{textLengthString} ");
                }
                else
                {
                    if (colType.Id == DBType.Decimal.Id)
                    {
                        if (column.NumericPrecision != null && column.NumericScale != null)
                            sb.Append($"{sqlType}({column.NumericPrecision},{column.NumericScale}) ");
                        else
                            sb.Append($"{sqlType} ");
                    }
                    else
                        sb.Append($"{sqlType} ");
                }

                if (column.Constraints != null)
                    foreach (var constraint in column.Constraints)
                        sb.Append($"{ParseExpression(constraint)} ");
                sb.AppendLine(",");
            }

            if (sqlCreateTable.Constraints != null)
                foreach (var constraint in sqlCreateTable.Constraints)
                    sb.Append(ParseExpression(constraint)).AppendLine(",");

            sb.Length -= $",{Environment.NewLine}".Length;
            sb.AppendLine(");");
            return sb.ToString();
        }
    }
}