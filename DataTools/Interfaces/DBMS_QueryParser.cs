using DataTools.DDL;
using DataTools.DML;
using System;
using System.Collections;
using System.Text;

namespace DataTools.Interfaces
{
    public abstract class DBMS_QueryParser : IDBMS_QueryParser
    {
        public SqlParameter[] CurrentParameters { get; private set; }

        public SqlExpression SimplifyQuery(SqlExpression query)
        {
            var q = ParseExpression(query);

            SqlComposition composition = new SqlComposition();
            var compositionElements = composition.Elements;
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
                        compositionElements.Add(new SqlCustom(q.Substring(from, pos - from)));
                    var parName = q.Substring(pos + 2, q.IndexOf('>', pos) - pos - 2);
                    compositionElements.Add(new SqlParameter(parName));
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
                }
                else pos++;

            }

            if (from < pos)
                compositionElements.Add(new SqlCustom(q.Substring(from, pos - from + 1)));

            return composition;
        }

        public string ToString(SqlExpression query) => ToString(query, parameters: null);

        public string ToString(SqlExpression query, params SqlParameter[] parameters)
        {
            CurrentParameters = parameters;
            return ParseExpression(query);
        }

        /// <summary>
        /// Функция преобразования значения в строковый литерал, удобный для чтения на стороне СУБД
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected abstract string StringifyValue(object value);

        protected string ParseExpression(SqlExpression expression)
        {
            switch (expression)
            {
                case DML.SqlSelect sqlSelect: return Parse_SqlSelect(sqlSelect);
                case DML.SqlInsert sqlInsert: return Parse_SqlInsert(sqlInsert);
                case DML.SqlInsertBatch sqlInsertBatch: return Parse_SqlInsertBatch(sqlInsertBatch);
                case DML.SqlDelete sqlDelete: return Parse_SqlDelete(sqlDelete);
                case DML.SqlUpdate sqlUpdate: return Parse_SqlUpdate(sqlUpdate);
                case DML.SqlName sqlName: return Parse_SqlName(sqlName);
                case DML.SqlConstant sqlConstant: return Parse_SqlConstant(sqlConstant);
                case DML.SqlExpressionWithAlias sqlExpressionWithAlias: return Parse_SqlExpressionWithAlias(sqlExpressionWithAlias);
                case DML.SqlCustom sqlCustom: return Parse_SqlCustom(sqlCustom);
                case DML.SqlWhere sqlWhereClause: return Parse_SqlWhereClause(sqlWhereClause);
                case DML.SqlOrderByClause sqlOrderByClause: return Parse_SqlOrderByClause(sqlOrderByClause);
                case DML.SqlFunction sqlFunction: return Parse_SqlFunction(sqlFunction);
                case DML.SqlProcedure sqlProcedure: return Parse_SqlProcedure(sqlProcedure);
                case DML.SqlGreaterThan sqlGreaterThan: return Parse_SqlGreaterThan(sqlGreaterThan);
                case DML.SqlGreaterOrEqual sqlGreaterOrEqual: return Parse_SqlGreaterOrEqual(sqlGreaterOrEqual);
                case DML.SqlLessThan sqlLesserThan: return Parse_SqlLesserThan(sqlLesserThan);
                case DML.SqlLessOrEqual sqlLesserOrEqual: return Parse_SqlLesserOrEqual(sqlLesserOrEqual);
                case DML.SqlEqual sqlEqual: return Parse_SqlEqual(sqlEqual);
                case DML.SqlNotEqual sqlNotEqual: return Parse_SqlNotEqual(sqlNotEqual);
                case DML.SqlIsNull sqlIsNull: return Parse_SqlIsNull(sqlIsNull);
                case DML.SqlAnd sqlAnd: return Parse_SqlAnd(sqlAnd);
                case DML.SqlOr sqlOr: return Parse_SqlOr(sqlOr);
                case DML.SqlNot sqlNot: return Parse_SqlNot(sqlNot);
                case DML.SqlComposition sqlComposition: return Parse_SqlComposition(sqlComposition);
                case DML.SqlParameter sqlParameter: return Parse_SqlParameter(sqlParameter);

                case DDL.SqlCreateTable sqlCreateTable: return Parse_SqlCreateTable(sqlCreateTable);
                case DDL.SqlDropTable sqlDropTable: return Parse_SqlDropTable(sqlDropTable);
                case DDL.SqlColumnAutoincrement sqlColumnAutoincrement: return Parse_SqlColumnAutoincrement(sqlColumnAutoincrement);
                case DDL.SqlTablePrimaryKey sqlTablePrimaryKey: return Parse_SqlTablePrimaryKey(sqlTablePrimaryKey);
                case DDL.SqlTableUnique sqlTableUnique: return Parse_SqlTableUnique(sqlTableUnique);
                case DDL.SqlTableForeignKey sqlTableForeignKey: return Parse_SqlTableForeignKey(sqlTableForeignKey);
                case DDL.SqlColumnNullable sqlColumnNullable: return Parse_SqlColumnNullable(sqlColumnNullable);
                default: throw new NotSupportedException($"{nameof(ParseExpression)}: expression is {expression.GetType()}. Unsupported.");
            }
        }

        protected abstract string Parse_SqlInsertBatch(SqlInsertBatch sqlInsertBatch);
        protected virtual string Parse_SqlColumnNullable(SqlColumnNullable sqlColumnNullable) => sqlColumnNullable.IsNullable ? "NULL" : "NOT NULL";
        protected virtual string Parse_SqlTableForeignKey(SqlTableForeignKey sqlTableForeignKey) => $"FOREIGN KEY ({string.Join(",", sqlTableForeignKey.Columns)}) REFERENCES {sqlTableForeignKey.ForeignTableName}({string.Join(",", sqlTableForeignKey.ForeignColumns)})";
        protected virtual string Parse_SqlTableUnique(SqlTableUnique sqlTableUnique) => $"UNIQUE ({string.Join(",", sqlTableUnique.Columns)})";
        protected virtual string Parse_SqlTablePrimaryKey(SqlTablePrimaryKey sqlTablePrimaryKey) => $"PRIMARY KEY ({string.Join(",", sqlTablePrimaryKey.Columns)})";
        protected abstract string Parse_SqlColumnAutoincrement(SqlColumnAutoincrement sqlColumnAutoincrement);
        protected abstract string Parse_SqlDropTable(SqlDropTable sqlDropTable);
        protected abstract string Parse_SqlCreateTable(SqlCreateTable sqlCreateTable);
        protected abstract string Parse_SqlUpdate(SqlUpdate sqlUpdate);
        protected abstract string Parse_SqlDelete(SqlDelete sqlDelete);
        protected abstract string Parse_SqlInsert(SqlInsert sqlInsert);
        protected abstract string Parse_SqlSelect(SqlSelect sqlSelect);
        protected abstract string Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias);
        protected abstract string Parse_SqlProcedure(SqlProcedure sqlProcedure);
        protected abstract string Parse_SqlFunction(SqlFunction sqlFunction);
        protected abstract string Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause);
        protected virtual string Parse_SqlWhereClause(SqlWhere sqlWhereClause)
        {
            var sb = new StringBuilder();
            sb.Append("(");
            foreach (var el in sqlWhereClause.Nodes)
                sb.Append(ParseExpression(el)).Append(" ");
            sb.Append(")");
            return sb.ToString();
        }

        protected virtual string Parse_SqlConstant(SqlConstant sqlConstant)
        {
            if (sqlConstant.Value is SqlExpression sqlExpression)
                return ParseExpression(sqlExpression);
            else return StringifyValue(sqlConstant.Value);
        }

        protected virtual string Parse_SqlCustom(SqlCustom sqlCustomQuery) => sqlCustomQuery.Query;

        protected virtual string Parse_SqlName(SqlName sqlName) => sqlName.Name;

        protected virtual string Parse_SqlOr(SqlOr sqlOr) => "OR";

        protected virtual string Parse_SqlAnd(SqlAnd sqlAnd) => "AND";

        protected virtual string Parse_SqlIsNull(SqlIsNull sqlIsNull) => "IS NULL";

        protected virtual string Parse_SqlEqual(SqlEqual sqlEqual) => "=";

        protected virtual string Parse_SqlLesserOrEqual(SqlLessOrEqual sqlLesserOrEqual) => "<=";

        protected virtual string Parse_SqlLesserThan(SqlLessThan sqlLesserThan) => "<";

        protected virtual string Parse_SqlGreaterOrEqual(SqlGreaterOrEqual sqlGreaterOrEqual) => ">=";

        protected virtual string Parse_SqlGreaterThan(SqlGreaterThan sqlGreaterThan) => ">";

        protected virtual string Parse_SqlNot(SqlNot sqlNot) => "NOT";

        protected virtual string Parse_SqlNotEqual(SqlNotEqual sqlNotEqual) => "<>";

        protected virtual string Parse_SqlComposition(SqlComposition sqlComposition)
        {
            var sb = new StringBuilder();
            foreach (var el in sqlComposition.Elements)
                sb.Append(ParseExpression(el));
            return sb.ToString();
        }

        protected virtual string Parse_SqlParameter(DML.SqlParameter sqlParameter)
        {
            string parName = sqlParameter.Name;
            var pars = CurrentParameters;
            if (pars != null)
                for (var i = 0; i < pars.Length; i++)
                {
                    var par = pars[i];
                    if (parName == par.Name)
                        return StringifyValue(par.Value);
                }
            return $"<{sqlParameter}>";
        }
    }
}

