using DataTools.DML;
using System.Text;

namespace DataTools.Interfaces
{
    public abstract class DBMS_QueryParser : IDBMSQueryParser
    {
        protected StringBuilder _query = new StringBuilder();

        protected SqlExpression _currentQuery;
        protected SqlParameter[] _currentParams;

        public string ToString(SqlExpression query, params SqlParameter[] parameters)
        {
            _BeforeParsing(query, parameters);
            _Parse(query);
            return _query.ToString();
        }

        void _BeforeParsing(SqlExpression query, params SqlParameter[] parameters)
        {
            _query.Clear();
            _currentQuery = query;
            _currentParams = parameters;
        }

        void _Parse(SqlExpression expression)
        {
            ParseExpression(expression);
        }

        /// <summary>
        /// Функция приведения и форматирования значения для передачи в источник данных
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected abstract string StringifyValue(object value);

        protected void ParseExpression(SqlExpression expression)
        {
            switch (expression)
            {
                case SqlSelect sqlSelect:
                    Parse_SqlSelect(sqlSelect);
                    break;
                case SqlInsert sqlInsert:
                    Parse_SqlInsert(sqlInsert);
                    break;
                case SqlDelete sqlDelete:
                    Parse_SqlDelete(sqlDelete);
                    break;
                case SqlUpdate sqlUpdate:
                    Parse_SqlUpdate(sqlUpdate);
                    break;
                case SqlName sqlName:
                    Parse_SqlName(sqlName);
                    break;
                case SqlConstant sqlConstant:
                    Parse_SqlConstant(sqlConstant);
                    break;
                case SqlExpressionWithAlias sqlExpressionWithAlias:
                    Parse_SqlExpressionWithAlias(sqlExpressionWithAlias);
                    break;
                case SqlCustom sqlCustom:
                    Parse_SqlCustom(sqlCustom);
                    break;
                case SqlWhereClause sqlWhereClause:
                    Parse_SqlWhereClause(sqlWhereClause);
                    break;
                case SqlOrderByClause sqlOrderByClause:
                    Parse_SqlOrderByClause(sqlOrderByClause);
                    break;
                case SqlFunction sqlFunction:
                    Parse_SqlFunction(sqlFunction);
                    break;
                case SqlProcedure sqlProcedure:
                    Parse_SqlProcedure(sqlProcedure);
                    break;
                case SqlGreaterThan sqlGreaterThan:
                    Parse_SqlGreaterThan(sqlGreaterThan);
                    break;
                case SqlGreaterOrEqual sqlGreaterOrEqual:
                    Parse_SqlGreaterOrEqual(sqlGreaterOrEqual);
                    break;
                case SqlLessThan sqlLesserThan:
                    Parse_SqlLesserThan(sqlLesserThan);
                    break;
                case SqlLessOrEqual sqlLesserOrEqual:
                    Parse_SqlLesserOrEqual(sqlLesserOrEqual);
                    break;
                case SqlEqual sqlEqual:
                    Parse_SqlEqual(sqlEqual);
                    break;
                case SqlNotEqual sqlNotEqual:
                    Parse_SqlNotEqual(sqlNotEqual);
                    break;
                case SqlIsNull sqlIsNull:
                    Parse_SqlIsNull(sqlIsNull);
                    break;
                case SqlAnd sqlAnd:
                    Parse_SqlAnd(sqlAnd);
                    break;
                case SqlOr sqlOr:
                    Parse_SqlOr(sqlOr);
                    break;
                case SqlNot sqlNot:
                    Parse_SqlNot(sqlNot);
                    break;
                case SqlComposition sqlComposition:
                    Parse_SqlComposition(sqlComposition);
                    break;
                case SqlParameter sqlParameter:
                    Parse_SqlParameter(sqlParameter);
                    break;
                default:
                    break;
            }
        }

        protected abstract void Parse_SqlParameter(SqlParameter sqlParameter);
        protected abstract void Parse_SqlComposition(SqlComposition sqlComposition);
        protected abstract void Parse_SqlNotEqual(SqlNotEqual sqlNotEqual);
        protected abstract void Parse_SqlNot(SqlNot sqlNot);
        protected abstract void Parse_SqlOr(SqlOr sqlOr);
        protected abstract void Parse_SqlAnd(SqlAnd sqlAnd);
        protected abstract void Parse_SqlIsNull(SqlIsNull sqlIsNull);
        protected abstract void Parse_SqlEqual(SqlEqual sqlEqual);
        protected abstract void Parse_SqlLesserOrEqual(SqlLessOrEqual sqlLesserOrEqual);
        protected abstract void Parse_SqlLesserThan(SqlLessThan sqlLesserThan);
        protected abstract void Parse_SqlGreaterOrEqual(SqlGreaterOrEqual sqlGreaterOrEqual);
        protected abstract void Parse_SqlGreaterThan(SqlGreaterThan sqlGreaterThan);
        protected abstract void Parse_SqlProcedure(SqlProcedure sqlProcedure);
        protected abstract void Parse_SqlFunction(SqlFunction sqlFunction);
        protected abstract void Parse_SqlOrderByClause(SqlOrderByClause sqlOrderByClause);
        protected abstract void Parse_SqlWhereClause(SqlWhereClause sqlWhereClause);
        protected abstract void Parse_SqlCustom(SqlCustom sqlCustom);
        protected abstract void Parse_SqlExpressionWithAlias(SqlExpressionWithAlias sqlExpressionWithAlias);
        protected abstract void Parse_SqlConstant(SqlConstant sqlConstant);
        protected abstract void Parse_SqlName(SqlName sqlName);
        protected abstract void Parse_SqlUpdate(SqlUpdate sqlUpdate);
        protected abstract void Parse_SqlDelete(SqlDelete sqlDelete);
        protected abstract void Parse_SqlInsert(SqlInsert sqlInsert);
        protected abstract void Parse_SqlSelect(SqlSelect sqlSelect);


    }
}

