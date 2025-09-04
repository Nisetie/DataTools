using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlSelectExtensions
    {

        public static SqlSelect Select(this SqlSelect sql, params string[] selects) => sql.Select(selects.Select(s => new SqlCustom(s)).ToArray());
        /// <summary>
        /// Возвращается подготовленная команда Select: select <see cref="ModelMetadata.Fields"/> from <typeparamref name="ModelT"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// /// <summary>
        public static SqlSelect From<ModelT>(this SqlSelect sqlSelect) where ModelT : class, new()
        {
            return From(sqlSelect, ModelMetadata<ModelT>.Instance);
        }

        public static SqlSelect From(this SqlSelect sqlSelect, IModelMetadata metadata)
        {
            sqlSelect.From(metadata.FullObjectName).Select(metadata.GetColumnsForSelect().Select(colName => new SqlName(colName)).ToArray());
            return sqlSelect;
        }

        public static SqlSelect From<ModelT>(this SqlSelect sqlSelect, SqlExpression subquery, string alias) where ModelT : class, new()
        {
            return From(sqlSelect, ModelMetadata<ModelT>.Instance, subquery, alias);
        }
        public static SqlSelect From(this SqlSelect sqlSelect, IModelMetadata modelMetadata, SqlExpression subquery, string alias)
        {
            sqlSelect.From(subquery, alias).Select(modelMetadata.GetColumnsForSelect().Select(colName => new SqlName(colName)).ToArray());
            return sqlSelect;
        }
        public static SqlSelect From(this SqlSelect sql, string objectName) => sql.From(new SqlName(objectName));
        public static SqlSelect From(this SqlSelect sql, SqlExpression subquery, string alias) => sql.From(new SqlExpressionWithAlias(subquery, alias));
        public static SqlSelect Where(this SqlSelect sql, string columnName, object value) => sql.Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));

        public static SqlSelect OrderBy(this SqlSelect sql, params string[] columnNames)
        {
            if (columnNames == null || columnNames.Length == 0)
                return sql;
            else
                return sql.OrderBy(columnNames.Select(cn => new SqlOrderByClause(new SqlName(cn))).ToArray());
        }

        public static SqlSelect OrderBy(this SqlSelect sql, params SqlExpression[] custom)
        {
            if (custom == null || custom.Length == 0)
                return sql;
            else
                return sql.OrderBy(custom.Select(cn => new SqlOrderByClause(cn)).ToArray());
        }
    }
}

