using DataTools.Common;
using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Linq;

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

        protected override void Parse_SqlSelect(SqlSelect sqlSelect)
        {
            _queryBuilder.Append("SELECT ");
            if (sqlSelect.Selects == null)
                _queryBuilder.Append("*");
            else
            {
                foreach (var s in sqlSelect.Selects)
                {
                    ParseExpression(s);
                    _queryBuilder.Append(',');
                }
                _queryBuilder.Length -= 1;
            }
            _queryBuilder.AppendLine();

            if (sqlSelect.FromSource == null) return; // select без источника (select getdate())

            _queryBuilder.AppendLine("FROM ");
            if (sqlSelect.FromSource is SqlSelect sqlSelect1)
            {
                _queryBuilder.Append("(");
                Parse_SqlSelect(sqlSelect1);
                _queryBuilder.Length -= 1; //remove ';'
                _queryBuilder.Append(")");
            }
            else
            {
                var name = sqlSelect.FromSource.ToString();
                if (name.IndexOf('.') == name.LastIndexOf('.'))
                    name = name.Replace('.', '_');
                _queryBuilder.Append(name);
            }
            _queryBuilder.AppendLine();


            if (sqlSelect.Wheres != null)
            {
                _queryBuilder.Append("WHERE ");
                ParseExpression(sqlSelect.Wheres);
                _queryBuilder.AppendLine();
            }

            if (sqlSelect.Orders != null)
            {
                _queryBuilder.Append("ORDER BY ");

                foreach (var o in sqlSelect.Orders)
                {
                    ParseExpression(o);
                    _queryBuilder.Append(',');
                }
                _queryBuilder.Length -= 1;
                _queryBuilder.AppendLine();
            }


            if (sqlSelect.LimitRows != null)
            {
                _queryBuilder.Append("LIMIT ");
                ParseExpression(sqlSelect.LimitRows);

                if (sqlSelect.OffsetRows != null)
                {
                    _queryBuilder.AppendLine(" OFFSET");
                    ParseExpression(sqlSelect.OffsetRows);
                }
            }
            _queryBuilder.Append(";");
        }
        protected override void Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias)
        {
            _queryBuilder.Append($"(");
            ParseExpression(sqlExpressionWithAlias.SqlExpression);
            if (_queryBuilder[_queryBuilder.Length - 1] == ';')
                _queryBuilder.Length -= 1;
            _queryBuilder.Append($") as {sqlExpressionWithAlias.Alias}");
        }

        protected override void Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause)
        {
            ParseExpression(sqlOrderByClause.OrderValue);
            _queryBuilder.Append($" {sqlOrderByClause.Order}");
        }

        protected override void Parse_SqlDelete(SqlDelete sqlDelete)
        {
            _queryBuilder.Append("DELETE FROM ");

            var name = sqlDelete.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _queryBuilder.Append(name).AppendLine();
            if (sqlDelete.Wheres != null)
            {
                _queryBuilder.Append("WHERE ");
                ParseExpression(sqlDelete.Wheres);
            }
            _queryBuilder.AppendLine(";");
        }

        protected override void Parse_SqlInsert(SqlInsert sqlInsert)
        {
            _queryBuilder.Append("INSERT INTO ");

            var name = sqlInsert.IntoDestination.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _queryBuilder.Append(name).Append("(");

            foreach (var c in sqlInsert.Columns)
            {
                ParseExpression(c);
                _queryBuilder.Append(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 1, 1)
                .AppendLine(")")
                .AppendLine("values")
                .Append("(");
            foreach (var v in sqlInsert.Values)
            {
                ParseExpression(v);
                _queryBuilder.Append(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 1, 1)
                .AppendLine("),")
                .Remove(_queryBuilder.Length - 3, 3)
                .AppendLine()
                .Append("returning *;");
        }

        protected override void Parse_SqlInsertBatch(SqlInsertBatch sqlInsertBatch)
        {
            _queryBuilder.Append("INSERT INTO ");

            var name = sqlInsertBatch.IntoDestination.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _queryBuilder.Append(name).Append("(");

            foreach (var c in sqlInsertBatch.Columns)
            {
                ParseExpression(c);
                _queryBuilder.Append(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 1, 1)
                .AppendLine(")")
                .AppendLine("values");


            foreach (var valuesRow in sqlInsertBatch.Values)
            {
                _queryBuilder.Append("(");
                foreach (var value in valuesRow)
                {
                    ParseExpression(value);
                    _queryBuilder.Append(",");
                }
                _queryBuilder.Length -= 1;
                _queryBuilder.AppendLine("),");
            }
            _queryBuilder.Length -= 3;
            _queryBuilder
                .AppendLine()
                .Append("returning *;");
        }

        protected override void Parse_SqlUpdate(SqlUpdate sqlUpdate)
        {
            _queryBuilder.Append("UPDATE ");

            var name = sqlUpdate.FromSource.ToString();
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _queryBuilder
                .Append(name)
                .AppendLine()
                .AppendLine("SET");


            var columnsE = sqlUpdate.Columns.GetEnumerator();
            var valuesE = sqlUpdate.Values.GetEnumerator();
            while (columnsE.MoveNext() && valuesE.MoveNext())
            {
                ParseExpression(columnsE.Current);
                _queryBuilder.Append("=");
                ParseExpression(valuesE.Current);
                _queryBuilder.AppendLine(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 3, 3)
                .AppendLine();

            if (sqlUpdate.Wheres != null)
            {
                _queryBuilder.Append("WHERE ");
                ParseExpression(sqlUpdate.Wheres);
                _queryBuilder.AppendLine();
            }
            _queryBuilder.Append(";");
        }

        protected override void Parse_SqlFunction(SqlFunction sqlFunction)
        {
            _queryBuilder.Append($"{sqlFunction.FunctionName}( ");
            foreach (var p in sqlFunction.Parameters)
            {
                ParseExpression(p);
                _queryBuilder.Append(",");
            }
            _queryBuilder.Remove(_queryBuilder.Length - 1, 1).Append(")");
        }

        protected override void Parse_SqlProcedure(SqlProcedure sqlProcedure)
        {
            throw new NotImplementedException();
        }

        protected override void Parse_SqlTableForeignKey(SqlTableForeignKey sqlTableForeignKey)
        {
            _queryBuilder.Append($"FOREIGN KEY ({string.Join(",", sqlTableForeignKey.Columns)}) REFERENCES {sqlTableForeignKey.ForeignTableName.Replace(".", "_")}({string.Join(",", sqlTableForeignKey.ForeignColumns)})");
        }

        protected override void Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement)
        {
            _queryBuilder.Append($"INTEGER PRIMARY KEY");
        }

        protected override void Parse_SqlDropTable(SqlDropTable sqlDropTable)
        {
            _queryBuilder.Append($"drop table if exists ");
            Parse_SqlName(sqlDropTable.TableName);
            _queryBuilder.Append($";");
        }

        protected override void Parse_SqlName(SqlName sqlName)
        {
            string name = sqlName.Name;
            name = name.Replace('.', '_');
            _queryBuilder.Append(name.Contains(" ") ? $"[{name}]" : name);
        }

        protected override void Parse_SqlCreateTable(SqlCreateTable sqlCreateTable)
        {
            var name = sqlCreateTable.TableName.Name;
            if (name.IndexOf('.') == name.LastIndexOf('.'))
                name = name.Replace('.', '_');

            _queryBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {name} (");

            int sbLength = default;
            foreach (var column in sqlCreateTable.Columns)
            {
                sbLength = _queryBuilder.Length;
                Parse_SqlName(column.ColumnName);
                _queryBuilder.Append(" ");

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
                    _queryBuilder.Append($"{sqlType}{textLengthString} ");
                }
                else
                {
                    if (colType.Id == DBType.Decimal.Id)
                    {
                        if (column.NumericPrecision != null && column.NumericScale != null)
                            _queryBuilder.Append($"{sqlType}({column.NumericPrecision},{column.NumericScale}) ");
                        else
                            _queryBuilder.Append($"{sqlType} ");
                    }
                    else
                        _queryBuilder.Append($"{sqlType} ");
                }

                if (column.Constraints != null)
                    foreach (var constraint in column.Constraints)
                    {
                        // если объявлен автоинкремент на уровне колонки и первичный целочисленный ключ на уровне таблицы,
                        // тогда пропустить ограничение автоинкремента для исключения дубля первичного ключа
                        if (constraint is SqlColumnAutoincrement && sqlCreateTable.Constraints != null && sqlCreateTable.Constraints.Any(tablec => (tablec is SqlTablePrimaryKey sqlTablePrimaryKey && sqlTablePrimaryKey.Columns.Any(pkc => pkc == column.ColumnName.Name)))) continue;

                        ParseExpression(constraint);
                        _queryBuilder.Append(" ");
                    }

                string colDef = _queryBuilder.ToString();
                int firstIndex = colDef.IndexOf("PRIMARY KEY", sbLength);
                if (firstIndex != -1)
                {
                    // удалить все лишние ограничения для автоинкрементного ID
                    if (colDef.IndexOf("INTEGER", sbLength) > 0)
                    {
                        _queryBuilder.Length -= _queryBuilder.Length - sbLength;
                        Parse_SqlName(column.ColumnName);
                        _queryBuilder.Append($" INTEGER PRIMARY KEY");
                    }
                }
                _queryBuilder.AppendLine($",");
            }
            if (sqlCreateTable.Constraints != null)
                foreach (var constraint in sqlCreateTable.Constraints)
                {
                    ParseExpression(constraint);
                    _queryBuilder.AppendLine(",");
                }

            _queryBuilder.Length -= $",{Environment.NewLine}".Length;
            _queryBuilder.Append(");");
        }
    }
}