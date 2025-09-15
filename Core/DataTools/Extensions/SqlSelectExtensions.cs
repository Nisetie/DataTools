using DataTools.Commands;
using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace DataTools.Extensions
{
    public static class SqlSelectExtensions
    {
        public static SqlSelect From<ModelT>(this SqlSelect sql) where ModelT : class, new() => From(sql, ModelMetadata<ModelT>.Instance);
        public static SqlSelect From(this SqlSelect sql, IModelMetadata metadata) => From(sql, objectName: metadata.FullObjectName);
        public static SqlSelect From(this SqlSelect sql, string objectName) => From(sql, new SqlName(objectName));
        public static SqlSelect From(this SqlSelect sql, ISqlExpression subquery, string alias) => From(sql, new SqlExpressionWithAlias(subquery, alias));
        public static SqlSelect From(this SqlSelect sql, ISqlExpression subquery) => sql.From(subquery);

        public static SqlSelect Select(this SqlSelect sql, params string[] selects) => sql.Select(selects.Select(s => new SqlCustom(s)).ToArray());
        public static SqlSelect Select<ModelT>(this SqlSelect sql) where ModelT : class, new() => Select(sql, ModelMetadata<ModelT>.Instance);
        public static SqlSelect Select(this SqlSelect sql, IModelMetadata modelMetadata) => sql.Select(modelMetadata.GetColumnsForSelect().Select(colName => new SqlName(colName)).ToArray());

        public static SqlSelect Where
            (this SqlSelect sql, string columnName, object value)
            => value == null
            ? sql.Where(new SqlWhere(new SqlName(columnName)).IsNull())
            : sql.Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));
        public static SqlSelect Where(this SqlSelect sql, IModelMetadata modelMetadata, dynamic model) => sql.Where(DynamicMapper.GetMapper(modelMetadata).GetWhereClause(model));
        public static SqlSelect Where<ModelT>(this SqlSelect sql, ModelT model) where ModelT : class, new() => sql.Where(ModelMapper<ModelT>.GetWhereClause(model));
        public static SqlSelect Where<ModelT>
            (this SqlSelect sql, Expression<Func<ModelT, bool>> filterExpression)
            where ModelT : class, new()
            => sql.Where(new SqlWhere(SelectCommandHelper.ProcessExpression(ModelMetadata<ModelT>.Instance, filterExpression.Body)));

        public static SqlSelect OrderBy
            (this SqlSelect sql, params string[] columnNames)
            => columnNames == null || columnNames.Length == 0
            ? sql
            : sql.OrderBy(columnNames.Select(cn => new SqlOrderByClause(new SqlName(cn))).ToArray());

        public static SqlSelect OrderBy
            (this SqlSelect sql, params ISqlExpression[] custom)
            => custom == null || custom.Length == 0
            ? sql
            : sql.OrderBy(custom.Select(cn => new SqlOrderByClause(cn)).ToArray());
    }
}

