using DataTools.Common;
using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Linq;
using System.Text;

namespace DataTools.SQLite
{
    public class SQLite_QueryParser : DBMS_QueryParser
    {

        static SQLite_QueryParser()
        {

        }

        protected override string StringifyValue(object value)
        {
            return SQLite_TypesMapper.ToStringSQL(value);
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
            else
            {
                var name = sqlSelect.FromSource.ToString();
                if (name.IndexOf('.') == name.LastIndexOf('.'))
                    name = name.Replace('.', '_');
                sb.Append(name);
            }
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
            sb.AppendLine(";");
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

        protected override string Parse_SqlInsertBatch(SqlInsertBatch sqlInsertBatch)
        {
            var sb = new StringBuilder(256);
            sb.Append("INSERT INTO ");

            var name = sqlInsertBatch.IntoDestination.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            sb.Append(name).Append("(");

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

        protected override string Parse_SqlTableForeignKey(SqlTableForeignKey sqlTableForeignKey)
        {
            return $"FOREIGN KEY ({string.Join(",", sqlTableForeignKey.Columns)}) REFERENCES {sqlTableForeignKey.ForeignTableName.Replace(".", "_")}({string.Join(",", sqlTableForeignKey.ForeignColumns)})";
        }

        protected override string Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement)
        {
            return $"INTEGER PRIMARY KEY";
        }

        protected override string Parse_SqlDropTable(SqlDropTable sqlDropTable)
        {
            var name = sqlDropTable.TableName.Name;
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');
            return $"drop table if exists {name};";
        }

        protected override string Parse_SqlName(SqlName sqlName)
        {
            string name = sqlName.Name;
            name = name.Replace('.', '_');
            return name.Contains(" ") ? $"[{name}]" : name;
        }

        protected override string Parse_SqlCreateTable(SqlCreateTable sqlCreateTable)
        {
            var sb = new StringBuilder(256);
            var colDefSb = new StringBuilder(64);

            var name = sqlCreateTable.TableName.Name;
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            sb.AppendLine($"CREATE TABLE IF NOT EXISTS {name} (");

            foreach (var column in sqlCreateTable.Columns)
            {
                colDefSb.Clear();


                colDefSb.Append($"{Parse_SqlName(column.ColumnName)} ");

                var colType = column.ColumnType;
                var sqlType = SQLite_TypesMapper.GetSqlType(colType);
                if (sqlType == null) throw new NullReferenceException($"{nameof(SQLite_QueryParser)}.{nameof(Parse_SqlCreateTable)}: {name}.{column.ColumnName} {colType}");

                if (colType.HasLength)
                {
                    var textLength = column.TextLength;
                    string textLengthString = string.Empty;
                    if (textLength != null)
                    {
                        if (textLength > 0)
                            textLengthString = $"({textLength})";
                    }
                    colDefSb.Append($"{sqlType}{textLengthString} ");
                }
                else
                {
                    if (colType.Id == DBType.Decimal.Id)
                    {
                        if (column.NumericPrecision != null && column.NumericScale != null)
                            colDefSb.Append($"{sqlType}({column.NumericPrecision},{column.NumericScale}) ");
                        else
                            colDefSb.Append($"{sqlType} ");
                    }
                    else
                        colDefSb.Append($"{sqlType} ");
                }

                if (column.Constraints != null)
                    foreach (var constraint in column.Constraints)
                    {
                        // если объявлен автоинкремент на уровне колонки и первичный целочисленный ключ на уровне таблицы,
                        // тогда пропустить ограничение автоинкремента для исключения дубля первичного ключа
                        if (constraint is SqlColumnAutoincrement && sqlCreateTable.Constraints != null && sqlCreateTable.Constraints.Any(tablec => (tablec is SqlTablePrimaryKey sqlTablePrimaryKey && sqlTablePrimaryKey.Columns.Any(pkc => pkc == column.ColumnName.Name)))) continue;

                        colDefSb.Append($"{ParseExpression(constraint)} ");
                    }

                string colDef = colDefSb.ToString();
                int firstIndex = colDef.IndexOf("PRIMARY KEY");
                if (firstIndex != -1)
                {
                    // удалить все лишние ограничения для автоинкрементного ID
                    if (colDef.Contains("INTEGER"))
                    {
                        colDef = $"{column.ColumnName} INTEGER PRIMARY KEY";
                    }
                }

                sb.AppendLine($"{colDef},");
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