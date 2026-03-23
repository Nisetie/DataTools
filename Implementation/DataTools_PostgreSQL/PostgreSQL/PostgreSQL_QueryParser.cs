using DataTools.Common;
using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using System;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_QueryParser : DBMS_QueryParser
    {
        private static PostgreSQL_QueryParser _instance;
        public static PostgreSQL_QueryParser Instance => _instance;
        static PostgreSQL_QueryParser()
        {

        }
        protected override string StringifyValue(object value)
        {
            return PostgreSQL_TypesMapper.ToStringSQL(value);
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
                _queryBuilder.Length -= 1; // remove ';'
                _queryBuilder.Append(")");
            }
            else
            {
                ParseExpression(sqlSelect.FromSource);
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

            if (sqlSelect.OffsetRows != null)
            {
                _queryBuilder.Append("OFFSET (");
                ParseExpression(sqlSelect.OffsetRows);
                _queryBuilder.AppendLine(") rows ");
                if (sqlSelect.LimitRows != null)
                {
                    _queryBuilder.AppendLine("fetch next (");
                    ParseExpression(sqlSelect.LimitRows);
                    _queryBuilder.AppendLine(") rows only");
                }
            }
            _queryBuilder.Append(";");
        }

        protected override void Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias)
        {
            if (sqlExpressionWithAlias.SqlExpression is SqlFunction)
            {
                ParseExpression(sqlExpressionWithAlias.SqlExpression);
                _queryBuilder.Append($" as {sqlExpressionWithAlias.Alias}");
            }
            else
            {
                _queryBuilder.Append("(");
                ParseExpression(sqlExpressionWithAlias.SqlExpression);
                if (_queryBuilder[_queryBuilder.Length - 1] == ';')
                    _queryBuilder.Length -= 1;
                _queryBuilder.Append($") as {sqlExpressionWithAlias.Alias}");
            }
        }

        protected override void Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause)
        {
            ParseExpression(sqlOrderByClause.OrderValue);
            _queryBuilder.Append($" {sqlOrderByClause.Order}");
        }

        protected override void Parse_SqlDelete(SqlDelete sqlDelete)
        {
            _queryBuilder.Append("DELETE FROM ");
            ParseExpression(sqlDelete.FromSource);
            _queryBuilder.AppendLine();
            if (sqlDelete.Wheres != null)
            {
                _queryBuilder.Append("WHERE ");
                ParseExpression(sqlDelete.Wheres);
            }
            _queryBuilder.AppendLine(";");
        }

        protected override void Parse_SqlInsertConstant(SqlInsertConstant sqlInsertConstant)
        {
            _queryBuilder.Append($"(");
            ParseExpression(sqlInsertConstant.Value);
            var sqlType = PostgreSQL_TypesMapper.GetSqlType(sqlInsertConstant.ValueDBType);
            _queryBuilder.Append($")::{sqlType}");
            if (sqlInsertConstant.ValueDBType.HasLength && sqlType != "text" && sqlType != "bytea")
            {
                if (sqlInsertConstant.TextLength != null && sqlInsertConstant.TextLength > 0)
                    _queryBuilder.Append($"({sqlInsertConstant.TextLength})");
            }
            else if (sqlInsertConstant.ValueDBType.HasPrecision && sqlType != "money")
            {
                if (sqlInsertConstant.NumericScale != null)
                    _queryBuilder.Append($"({sqlInsertConstant.NumericPrecision},{sqlInsertConstant.NumericScale})");
            }
        }

        protected override void Parse_SqlInsert(SqlInsert sqlInsert)
        {
            _queryBuilder.Append("INSERT INTO ");
            ParseExpression(sqlInsert.IntoDestination);
            _queryBuilder.Append("(");
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
            ParseExpression(sqlInsertBatch.IntoDestination);
            _queryBuilder.Append("(");
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
            _queryBuilder.AppendLine(";");
        }

        protected override void Parse_SqlUpdate(SqlUpdate sqlUpdate)
        {
            _queryBuilder.Append("UPDATE ");
            ParseExpression(sqlUpdate.FromSource);
            _queryBuilder.AppendLine().AppendLine("SET");

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
            _queryBuilder.AppendLine(";");
        }

        protected override void Parse_SqlFunction(SqlFunction sqlFunction)
        {
            _queryBuilder.Append($"{sqlFunction.FunctionName}( ");

            foreach (var p in sqlFunction.Parameters)
            {
                ParseExpression(p);
                _queryBuilder.Append(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 1, 1)
                .Append(")");
        }

        protected override void Parse_SqlProcedure(SqlProcedure sqlProcedure)
        {
            _queryBuilder.Append($"CALL {sqlProcedure.ProcedureName}( ");

            foreach (var p in sqlProcedure.Parameters)
            {
                ParseExpression(p);
                _queryBuilder.Append(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 1, 1)
                .Append(");");
        }

        protected override void Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement)
        {
            _queryBuilder.Append("GENERATED ALWAYS AS IDENTITY");
        }

        protected override void Parse_SqlDropTable(SqlDropTable sqlDropTable)
        {
            _queryBuilder.Append($"DROP TABLE IF EXISTS ");
            Parse_SqlName(sqlDropTable.TableName);
            _queryBuilder.Append($";");
        }

        protected override void Parse_SqlName(SqlName sqlName)
        {
            string name = sqlName.Name.ToLower();
            if (name.Contains(" "))
                _queryBuilder.Append($"\"{name}\"");
            else _queryBuilder.Append(name);
        }

        protected override void Parse_SqlDDLColumnDefinition(SqlDDLColumnDefinition column)
        {
            Parse_SqlName(column.ColumnName);
            _queryBuilder.Append(" ");

            var colType = column.ColumnType;
            var sqlType = PostgreSQL_TypesMapper.GetSqlType(colType);
            if (sqlType == null) throw new NullReferenceException($"Unknown sql type of {nameof(Parse_SqlCreateTable)}: {column.ColumnName} {colType}.\r\n{_queryBuilder}");

            if (colType.HasLength && sqlType != "text" && sqlType != "bytea")
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
                    ParseExpression(constraint);
                    _queryBuilder.Append(" ");
                }
        }

        protected override void Parse_SqlCreateTable(SqlCreateTable sqlCreateTable)
        {
            var name = sqlCreateTable.TableName.Name;

            _queryBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {sqlCreateTable.TableName} (");

            foreach (var column in sqlCreateTable.Columns)
            {
                Parse_SqlDDLColumnDefinition(column);
                _queryBuilder.AppendLine(",");
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