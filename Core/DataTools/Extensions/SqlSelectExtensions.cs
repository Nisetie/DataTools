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
        /// <summary>
        /// Указать источник данных в блоке FROM
        /// </summary>
        /// <typeparam name="ModelT"></typeparam>
        /// <param name="sqlSelect"></param>
        /// <returns></returns>
        public static SqlSelect From<ModelT>(this SqlSelect sqlSelect) where ModelT : class, new() => From(sqlSelect, ModelMetadata<ModelT>.Instance);
        public static SqlSelect From(this SqlSelect sqlSelect, IModelMetadata metadata) => From(sqlSelect, objectName: metadata.FullObjectName);
        public static SqlSelect From(this SqlSelect sqlSelect, string objectName) => From(sqlSelect, new SqlName(objectName));
        public static SqlSelect From(this SqlSelect sqlSelect, ISqlExpression subquery, string alias) => From(sqlSelect, new SqlExpressionWithAlias(subquery, alias));
        public static SqlSelect From(this SqlSelect sqlSelect, ISqlExpression subquery) => sqlSelect.From(subquery);
        public static SqlSelect Select(this SqlSelect sqlSelect, params string[] selects) => sqlSelect.Select(selects.Select(s => new SqlCustom(s)).ToArray());
        public static SqlSelect Select<ModelT>(this SqlSelect sqlSelect) where ModelT : class, new() => Select(sqlSelect, ModelMetadata<ModelT>.Instance);
        public static SqlSelect Select(this SqlSelect sqlSelect, IModelMetadata modelMetadata)
        {
            return sqlSelect.Select(MetadataHelper.GetColumnNamesFromColumnMetas(modelMetadata.GetColumnsForSelect()));
        }

        public static SqlSelect Where
            (this SqlSelect sqlSelect, string columnName, object value)
            => value == null
            ? sqlSelect.Where(new SqlWhere(new SqlName(columnName)).IsNull())
            : sqlSelect.Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));
        public static SqlSelect Where(this SqlSelect sqlSelect, IModelMetadata modelMetadata, dynamic model) => sqlSelect.Where(DynamicMapper.GetMapper(modelMetadata).GetWhereClause(model));
        public static SqlSelect Where<ModelT>(this SqlSelect sqlSelect, ModelT model) where ModelT : class, new() => sqlSelect.Where(ModelMapper<ModelT>.GetWhereClause(model));
        public static SqlSelect Where<ModelT>
            (this SqlSelect sqlSelect, Expression<Func<ModelT, bool>> filterExpression)
            where ModelT : class, new()
            => sqlSelect.Where(new SqlWhere(SelectCommandHelper.ProcessExpression(ModelMetadata<ModelT>.Instance, filterExpression.Body)));

        public static SqlSelect OrderBy
            (this SqlSelect sqlSelect, params string[] columnNames)
            => columnNames == null || columnNames.Length == 0
            ? sqlSelect
            : sqlSelect.OrderBy(columnNames.Select(cn => new SqlOrderByClause(new SqlName(cn))).ToArray());

        public static SqlSelect OrderBy
            (this SqlSelect sqlSelect, params ISqlExpression[] custom)
            => custom == null || custom.Length == 0
            ? sqlSelect
            : sqlSelect.OrderBy(custom.Select(cn => new SqlOrderByClause(cn)).ToArray());
    }
}

