using DataTools.DML;
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
            var meta = ModelMetadata<ModelT>.Instance;
            var fields = meta.Fields;
            var selectColumns = (from f in fields select new SqlName(f.ColumnName)).ToArray();
            var orderColumns = (from f in fields where f.IsSorted orderby f.SortOrder ascending select new SqlOrderByClause(new SqlName(f.ColumnName), f.SortDirection)).ToArray();
            sqlSelect
                .From(meta.FullObjectName)
                .Select(selectColumns);
            if (orderColumns.Length > 0)
                sqlSelect.OrderBy(orderColumns);
            return sqlSelect;
        }

        public static SqlSelect From<ModelT>(this SqlSelect sqlSelect, SqlExpression subquery, string alias) where ModelT : class, new()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            var fields = meta.Fields;
            var selectColumns = (from f in fields select new SqlName(f.ColumnName)).ToArray();
            var orderColumns = (from f in fields where f.IsSorted orderby f.SortOrder ascending select new SqlOrderByClause(new SqlName(f.ColumnName), f.SortDirection)).ToArray();
            sqlSelect
                .From(subquery, alias)
               .Select(selectColumns);
            if (orderColumns.Length > 0)
                sqlSelect.OrderBy(orderColumns);
            return sqlSelect;
        }


        public static SqlSelect From(this SqlSelect sql, string objectName) => sql.From(new SqlName(objectName));
        public static SqlSelect From(this SqlSelect sql, SqlExpression subquery, string alias) => sql.From(new SqlExpressionWithAlias(subquery, alias));

        /// <summary>
        /// Дополнить условие
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="columnName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static SqlSelect Where(this SqlSelect sql, string columnName, object value) => sql.Where(new SqlWhereClause(new SqlName(columnName)).Eq(new SqlConstant(value)));

        public static SqlSelect OrderBy(this SqlSelect sql, string columnName)
        {
            return sql.OrderBy(new SqlOrderByClause(new SqlName(columnName)));
        }
    }
}

