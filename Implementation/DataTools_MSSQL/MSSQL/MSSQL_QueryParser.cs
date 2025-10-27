using DataTools.Common;
using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using System;

namespace DataTools.MSSQL
{
    public sealed class MSSQL_QueryParser : DBMS_QueryParser
    {
        static MSSQL_QueryParser()
        {

        }

        protected override string StringifyValue(object value)
        {
            return MSSQL_TypesMapper.ToStringSQL(value);
        }

        protected override void Parse_SqlSelect(SqlSelect sqlSelect)
        {
            _queryBuilder.Append("SELECT ");

            if (sqlSelect.Selects != null)
                foreach (var s in sqlSelect.Selects)
                {
                    ParseExpression(s);
                    _queryBuilder.Append(',');
                }
            _queryBuilder.Length -= 1;

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
            else ParseExpression(sqlSelect.FromSource);
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
            else if (sqlSelect.OffsetRows != null)
                _queryBuilder.AppendLine("ORDER BY 1");

            if (sqlSelect.OffsetRows != null)
            {
                _queryBuilder.Append("OFFSET ");
                ParseExpression(sqlSelect.OffsetRows);
                _queryBuilder.AppendLine(" ROWS ");
                if (sqlSelect.LimitRows != null)
                {
                    _queryBuilder.AppendLine("FETCH NEXT ");
                    ParseExpression(sqlSelect.LimitRows);
                    _queryBuilder.AppendLine(" ROWS ONLY");
                }
            }
            _queryBuilder.AppendLine(";");
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
            _queryBuilder.Append($"cast(");
            ParseExpression(sqlInsertConstant.Value);
            _queryBuilder.Append($" as {MSSQL_TypesMapper.GetSqlType(sqlInsertConstant.ValueDBType)}");
            if (sqlInsertConstant.ValueDBType.HasLength)
            {
                if (sqlInsertConstant.TextLength != null && sqlInsertConstant.TextLength > 0)
                    _queryBuilder.Append($"({sqlInsertConstant.TextLength})");
                else
                    _queryBuilder.Append($"(max)");
            }
            else if (sqlInsertConstant.ValueDBType.HasPrecision)
            {
                if (sqlInsertConstant.NumericScale != null)
                    _queryBuilder.Append($"({sqlInsertConstant.NumericPrecision},{sqlInsertConstant.NumericScale})");
            }
            _queryBuilder.Append($")");
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
                .AppendLine("output inserted.*")
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
                .AppendLine(";");
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
                .AppendLine("output inserted.*")
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
            _queryBuilder.Append($"{sqlFunction.FunctionName}( "); // не удаляй пробел!
            foreach (var p in sqlFunction.Parameters)
            {
                ParseExpression(p);
                _queryBuilder.Append(",");
            }
            _queryBuilder.Length -= 1;
            _queryBuilder.Append(")");
        }

        protected override void Parse_SqlProcedure(SqlProcedure sqlProcedure)
        {
            _queryBuilder.Append($"EXEC {sqlProcedure.ProcedureName} ");
            foreach (var p in sqlProcedure.Parameters)
            {
                ParseExpression(p);
                _queryBuilder.Append(",");
            }
            _queryBuilder
                .Remove(_queryBuilder.Length - 1, 1)
                .Append(";");
        }

        protected override void Parse_SqlName(SqlName sqlName)
        {
            string name = sqlName.Name;
            if (name.Contains("."))
            {
                name = string.Join("].[", name.Split(new char[] { '.' }, options: StringSplitOptions.RemoveEmptyEntries));
                name = "[" + name + "]";
                _queryBuilder.Append(name);
            }
            else
                _queryBuilder.Append($"[{name}]");
        }

        protected override void Parse_SqlDDLColumnDefinition(SqlDDLColumnDefinition column)
        {
            Parse_SqlName(column.ColumnName);
            _queryBuilder.Append($" ");

            var colType = column.ColumnType;
            var sqlType = MSSQL_TypesMapper.GetSqlTypeFromType(column.ColumnType.Type);
            if (sqlType == null) throw new NullReferenceException($"Unknown sql type of {column.ColumnName} {colType}!");

            if (colType.HasLength)
            {
                var textLength = column.TextLength;
                string textLengthString = string.Empty;

                if (textLength != null && textLength > 0)
                    textLengthString = $"({textLength})";
                else
                    textLengthString = $"(max)";

                _queryBuilder.Append($"{sqlType}{textLengthString} ");
            }
            else if (colType.Id == DBType.Decimal.Id)
            {
                if (column.NumericPrecision != null && column.NumericScale != null)
                    _queryBuilder.Append($"{sqlType}({column.NumericPrecision},{column.NumericScale}) ");
                else
                    _queryBuilder.Append($"{sqlType}(38,19) ");
            }
            else _queryBuilder.Append($"{sqlType} ");

            if (column.Constraints != null)
                foreach (var constraint in column.Constraints)
                {
                    ParseExpression(constraint);
                    _queryBuilder.Append($" ");
                }
        }

        protected override void Parse_SqlCreateTable(SqlCreateTable sqlCreateTable)
        {
            _queryBuilder.AppendLine($"if (object_id('{sqlCreateTable.TableName}') is null) CREATE TABLE {sqlCreateTable.TableName} (");

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
            _queryBuilder.AppendLine(");");
        }

        protected override void Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement)
        {
            _queryBuilder.Append("IDENTITY");
        }

        protected override void Parse_SqlDropTable(SqlDropTable sqlDropTable)
        {
            _queryBuilder.Append($"drop table if exists ");
            Parse_SqlName(sqlDropTable.TableName);
            _queryBuilder.AppendLine($";");
        }
    }
}

