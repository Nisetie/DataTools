using DataTools.DDL;
using DataTools.DML;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataTools.Interfaces
{
    public abstract class DBMS_QueryParser : IDBMS_QueryParser
    {
        protected StringBuilder _queryBuilder = new StringBuilder();
        public SqlParameter[] CurrentParameters { get; private set; }


        public ISqlExpression SimplifyQuery(ISqlExpression query)
        {
            return SimplifyQuery(query, null);
        }
        public ISqlExpression SimplifyQuery(ISqlExpression query, params SqlParameter[] sqlParameters)
        {
            CurrentParameters = sqlParameters;
            _queryBuilder.Clear();
            ParseExpression(query);
            var q = _queryBuilder.ToString();

            List<ISqlExpression> expressions = new List<ISqlExpression>();
            int from = 0;
            int to = q.Length - 1;
            var pos = 0;
            char c; //currentChar
            int qc = 0; //quoteCount
            int qp; //quotePow
            while (pos < to)
            {
                c = q[pos];
                if (c == '<' && q[pos + 1] == '$' && qc == 0)
                {
                    if (pos > from)
                        expressions.Add(new SqlCustom(q.Substring(from, pos - from)));
                    var parName = q.Substring(pos + 2, q.IndexOf('>', pos) - pos - 2);
                    expressions.Add(new SqlParameter(parName));
                    pos += parName.Length + 3;
                    from = pos++;
                }
                else if (c == '\'')
                {
                    if (q.Substring(pos, (qp = 1 << qc)) == new string('\'', qp))
                        pos += (1 << (qc += 1));
                    else
                        if (q.Substring(pos, (qp = 1 << (qc - 1))) == new string('\'', qp))
                        pos += (1 << (qc -= 1));
                    else pos++;
                }
                else pos++;

            }

            if (from <= to)
                expressions.Add(new SqlCustom(q.Substring(from)));

            if (expressions.Count == 1)
                return expressions[0];
            else
                return new SqlComposition(expressions.ToArray());
        }

        public string ToString(ISqlExpression query) => ToString(query, parameters: null);

        public string ToString(ISqlExpression query, params SqlParameter[] parameters)
        {
            _queryBuilder.Clear();
            CurrentParameters = parameters;
            ParseExpression(query);
            return _queryBuilder.ToString();
        }

        /// <summary>
        /// Функция преобразования значения в строковый литерал, удобный для чтения на стороне СУБД
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected abstract string StringifyValue(object value);

        protected void ParseExpression(ISqlExpression expression)
        {
            switch (expression)
            {
                case DML.SqlSelect sqlSelect: Parse_SqlSelect(sqlSelect); break;
                case DML.SqlInsert sqlInsert: Parse_SqlInsert(sqlInsert); break;
                case DML.SqlInsertBatch sqlInsertBatch: Parse_SqlInsertBatch(sqlInsertBatch); break;
                case DML.SqlDelete sqlDelete: Parse_SqlDelete(sqlDelete); break;
                case DML.SqlUpdate sqlUpdate: Parse_SqlUpdate(sqlUpdate); break;
                case DML.SqlName sqlName: Parse_SqlName(sqlName); break;
                case DML.SqlConstant sqlConstant: Parse_SqlConstant(sqlConstant); break;
                case DML.SqlExpressionWithAlias sqlExpressionWithAlias: Parse_SqlExpressionWithAlias(sqlExpressionWithAlias); break;
                case DML.SqlCustom sqlCustom: Parse_SqlCustom(sqlCustom); break;
                case DML.SqlWhere sqlWhereClause: Parse_SqlWhereClause(sqlWhereClause); break;
                case DML.SqlOrderByClause sqlOrderByClause: Parse_SqlOrderByClause(sqlOrderByClause); break;
                case DML.SqlFunction sqlFunction: Parse_SqlFunction(sqlFunction); break;
                case DML.SqlProcedure sqlProcedure: Parse_SqlProcedure(sqlProcedure); break;

                case DML.SqlGreaterThan sqlGreaterThan: Parse_SqlGreaterThan(sqlGreaterThan); break;
                case DML.SqlGreaterOrEqual sqlGreaterOrEqual: Parse_SqlGreaterOrEqual(sqlGreaterOrEqual); break;
                case DML.SqlLessThan sqlLesserThan: Parse_SqlLesserThan(sqlLesserThan); break;
                case DML.SqlLessOrEqual sqlLesserOrEqual: Parse_SqlLesserOrEqual(sqlLesserOrEqual); break;
                case DML.SqlEqual sqlEqual: Parse_SqlEqual(sqlEqual); break;
                case DML.SqlNotEqual sqlNotEqual: Parse_SqlNotEqual(sqlNotEqual); break;
                case DML.SqlIsNull sqlIsNull: Parse_SqlIsNull(sqlIsNull); break;
                case DML.SqlAnd sqlAnd: Parse_SqlAnd(sqlAnd); break;
                case DML.SqlOr sqlOr: Parse_SqlOr(sqlOr); break;
                case DML.SqlNot sqlNot: Parse_SqlNot(sqlNot); break;

                case DML.SqlComposition sqlComposition: Parse_SqlComposition(sqlComposition); break;
                case DML.SqlParameter sqlParameter: Parse_SqlParameter(sqlParameter); break;
                case DML.SqlInsertConstant sqlInsertConstant: Parse_SqlInsertConstant(sqlInsertConstant); break;

                case DDL.SqlDDLColumnDefinition sqlColumnDefinition: Parse_SqlDDLColumnDefinition(sqlColumnDefinition); break;
                case DDL.SqlCreateTable sqlCreateTable: Parse_SqlCreateTable(sqlCreateTable); break;
                case DDL.SqlDropTable sqlDropTable: Parse_SqlDropTable(sqlDropTable); break;
                case DDL.SqlColumnAutoincrement sqlColumnAutoincrement: Parse_SqlColumnAutoincrement(sqlColumnAutoincrement); break;
                case DDL.SqlTablePrimaryKey sqlTablePrimaryKey: Parse_SqlTablePrimaryKey(sqlTablePrimaryKey); break;
                case DDL.SqlTableUnique sqlTableUnique: Parse_SqlTableUnique(sqlTableUnique); break;
                case DDL.SqlTableForeignKey sqlTableForeignKey: Parse_SqlTableForeignKey(sqlTableForeignKey); break;
                case DDL.SqlColumnNullable sqlColumnNullable: Parse_SqlColumnNullable(sqlColumnNullable); break;
                default: throw new NotSupportedException($"{nameof(ParseExpression)}: expression is {expression.GetType()}. Unsupported.");
            }
        }

        protected abstract void Parse_SqlInsertConstant(SqlInsertConstant sqlInsertConstant);
        protected abstract void Parse_SqlDDLColumnDefinition(SqlDDLColumnDefinition sqlColumnDefinition);
        protected abstract void Parse_SqlInsertBatch(SqlInsertBatch sqlInsertBatch);
        protected virtual void Parse_SqlColumnNullable(SqlColumnNullable sqlColumnNullable) => _queryBuilder.Append(sqlColumnNullable.IsNullable ? "NULL" : "NOT NULL");
        protected virtual void Parse_SqlTableForeignKey(SqlTableForeignKey sqlTableForeignKey) => _queryBuilder.Append($"FOREIGN KEY({string.Join(",", sqlTableForeignKey.Columns)}) REFERENCES {sqlTableForeignKey.ForeignTableName}({string.Join(",", sqlTableForeignKey.ForeignColumns)})");
        protected virtual void Parse_SqlTableUnique(SqlTableUnique sqlTableUnique) => _queryBuilder.Append($"UNIQUE({string.Join(",", sqlTableUnique.Columns)})");
        protected virtual void Parse_SqlTablePrimaryKey(SqlTablePrimaryKey sqlTablePrimaryKey) => _queryBuilder.Append($"PRIMARY KEY({string.Join(",", sqlTablePrimaryKey.Columns)})");
        protected abstract void Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement);
        protected abstract void Parse_SqlDropTable(SqlDropTable sqlDropTable);
        protected abstract void Parse_SqlCreateTable(SqlCreateTable sqlCreateTable);
        protected abstract void Parse_SqlUpdate(SqlUpdate sqlUpdate);
        protected abstract void Parse_SqlDelete(SqlDelete sqlDelete);
        protected abstract void Parse_SqlInsert(SqlInsert sqlInsert);
        protected abstract void Parse_SqlSelect(SqlSelect sqlSelect);
        protected abstract void Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias);
        protected abstract void Parse_SqlProcedure(SqlProcedure sqlProcedure);
        protected abstract void Parse_SqlFunction(SqlFunction sqlFunction);
        protected abstract void Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause);
        protected virtual void Parse_SqlWhereClause(SqlWhere sqlWhereClause)
        {
            _queryBuilder.Append("(");
            foreach (var el in sqlWhereClause.Nodes)
            {
                ParseExpression(el);
                _queryBuilder.Append(" ");
            }
            _queryBuilder.Append(")");
        }

        protected virtual void Parse_SqlConstant(SqlConstant sqlConstant)
        {
            if (sqlConstant.Value is ISqlExpression sqlExpression)
                ParseExpression(sqlExpression);
            else _queryBuilder.Append(StringifyValue(sqlConstant.Value));
        }

        protected virtual void Parse_SqlCustom(SqlCustom sqlCustomQuery) => _queryBuilder.Append(sqlCustomQuery.Query);

        protected virtual void Parse_SqlName(SqlName sqlName) => _queryBuilder.Append(sqlName.Name);

        protected virtual void Parse_SqlOr(SqlOr sqlOr) => _queryBuilder.Append("OR");

        protected virtual void Parse_SqlAnd(SqlAnd sqlAnd) => _queryBuilder.Append("AND");

        protected virtual void Parse_SqlIsNull(SqlIsNull sqlIsNull) => _queryBuilder.Append("IS NULL");

        protected virtual void Parse_SqlEqual(SqlEqual sqlEqual) => _queryBuilder.Append("=");

        protected virtual void Parse_SqlLesserOrEqual(SqlLessOrEqual sqlLesserOrEqual) => _queryBuilder.Append("<=");

        protected virtual void Parse_SqlLesserThan(SqlLessThan sqlLesserThan) => _queryBuilder.Append("<");

        protected virtual void Parse_SqlGreaterOrEqual(SqlGreaterOrEqual sqlGreaterOrEqual) => _queryBuilder.Append(">=");

        protected virtual void Parse_SqlGreaterThan(SqlGreaterThan sqlGreaterThan) => _queryBuilder.Append(">");

        protected virtual void Parse_SqlNot(SqlNot sqlNot) => _queryBuilder.Append("NOT");

        protected virtual void Parse_SqlNotEqual(SqlNotEqual sqlNotEqual) => _queryBuilder.Append("<>");

        protected virtual void Parse_SqlComposition(SqlComposition sqlComposition)
        {
            foreach (var el in sqlComposition.Elements)
                ParseExpression(el);
        }

        protected virtual void Parse_SqlParameter(DML.SqlParameter sqlParameter)
        {
            string parName = sqlParameter.Name;
            var pars = CurrentParameters;
            if (pars != null)
                for (var i = 0; i < pars.Length; i++)
                {
                    var par = pars[i];
                    if (parName == par.Name)
                    {
                        _queryBuilder.Append(StringifyValue(par.Value));
                        return;
                    }

                }
            _queryBuilder.Append($"<{sqlParameter}>");
        }
    }
}

