using DataTools.DML;
using System;
using System.Text;

namespace DataTools.Interfaces
{

    public abstract class DBMS_QueryParser : IDBMS_QueryParser
    {
        public SqlParameter[] CurrentParameters { get; private set; }

        /// <summary>
        /// Упрощение дерева выражений sql-запроса до комбинации SqlCustom и SqlParameter.
        /// Чтобы в итоге финальный текст запроса получался простой конкатенацией текста запроса и параметров.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public SqlExpression SimplifyQuery(SqlExpression query)
        {
            var q = ParseExpression(query);

            SqlComposition composition = new SqlComposition();
            var compositionElements = composition.Elements;
            int from = 0;
            var pos = 0;
            char c; //currentChar
            int qc = 0; //quoteCount
            int qp; //quotePow
            while (pos < q.Length)
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
                compositionElements.Add(new SqlCustom(q.Substring(from, pos - from)));

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
                case SqlSelect sqlSelect: return Parse_SqlSelect(sqlSelect);
                case SqlInsert sqlInsert: return Parse_SqlInsert(sqlInsert);
                case SqlDelete sqlDelete: return Parse_SqlDelete(sqlDelete);
                case SqlUpdate sqlUpdate: return Parse_SqlUpdate(sqlUpdate);
                case SqlName sqlName: return Parse_SqlName(sqlName);
                case SqlConstant sqlConstant: return Parse_SqlConstant(sqlConstant);
                case SqlExpressionWithAlias sqlExpressionWithAlias: return Parse_SqlExpressionWithAlias(sqlExpressionWithAlias);
                case SqlCustom sqlCustom: return Parse_SqlCustom(sqlCustom);
                case SqlWhereClause sqlWhereClause: return Parse_SqlWhereClause(sqlWhereClause);
                case SqlOrderByClause sqlOrderByClause: return Parse_SqlOrderByClause(sqlOrderByClause);
                case SqlFunction sqlFunction: return Parse_SqlFunction(sqlFunction);
                case SqlProcedure sqlProcedure: return Parse_SqlProcedure(sqlProcedure);
                case SqlGreaterThan sqlGreaterThan: return Parse_SqlGreaterThan(sqlGreaterThan);
                case SqlGreaterOrEqual sqlGreaterOrEqual: return Parse_SqlGreaterOrEqual(sqlGreaterOrEqual);
                case SqlLessThan sqlLesserThan: return Parse_SqlLesserThan(sqlLesserThan);
                case SqlLessOrEqual sqlLesserOrEqual: return Parse_SqlLesserOrEqual(sqlLesserOrEqual);
                case SqlEqual sqlEqual: return Parse_SqlEqual(sqlEqual);
                case SqlNotEqual sqlNotEqual: return Parse_SqlNotEqual(sqlNotEqual);
                case SqlIsNull sqlIsNull: return Parse_SqlIsNull(sqlIsNull);
                case SqlAnd sqlAnd: return Parse_SqlAnd(sqlAnd);
                case SqlOr sqlOr: return Parse_SqlOr(sqlOr);
                case SqlNot sqlNot: return Parse_SqlNot(sqlNot);
                case SqlComposition sqlComposition: return Parse_SqlComposition(sqlComposition);
                case SqlParameter sqlParameter: return Parse_SqlParameter(sqlParameter);
                default: throw new NotSupportedException($"{nameof(ParseExpression)}: expression is {expression.GetType()}. Unsupported.");
            }
        }

        protected abstract string Parse_SqlUpdate(SqlUpdate sqlUpdate);
        protected abstract string Parse_SqlDelete(SqlDelete sqlDelete);
        protected abstract string Parse_SqlInsert(SqlInsert sqlInsert);
        protected abstract string Parse_SqlSelect(SqlSelect sqlSelect);

        protected abstract string Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias);

        protected abstract string Parse_SqlProcedure(SqlProcedure sqlProcedure);
        protected abstract string Parse_SqlFunction(SqlFunction sqlFunction);

        protected abstract string Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause);

        protected virtual string Parse_SqlWhereClause(SqlWhereClause sqlWhereClause)
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
            return StringifyValue(sqlConstant.Value);
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
            var pars = CurrentParameters;
            if (pars != null)
                for (var i = 0; i < pars.Length; i++)
                {
                    var par = pars[i];
                    if (sqlParameter.Name == par.Name)
                        return StringifyValue(par.Value);
                }
            return $"<{sqlParameter}>";
        }
    }
}

