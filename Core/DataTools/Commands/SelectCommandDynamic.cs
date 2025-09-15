using DataTools.Common;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Commands
{
    public class SelectCommandDynamic : ISqlExpression, ISqlSelect<SelectCommandDynamic>
    {
        public IDataContext DataContext { get; }
        public IModelMetadata Metadata { get; }
        public SqlSelect Query { get; }
        public ISqlExpression FromSource => Query.FromSource;
        public ISqlExpression LimitRows => Query.LimitRows;
        public ISqlExpression OffsetRows => Query.OffsetRows;
        public IEnumerable<SqlOrderByClause> Orders => Query.Orders;
        public IEnumerable<ISqlExpression> Selects => Query.Selects;
        public SqlWhere Wheres => Query.Wheres;
        public SelectCommandDynamic(IDataContext dataContext, IModelMetadata modelMetadata)
        {
            DataContext = dataContext;
            Metadata = modelMetadata;
            Query = new SqlSelect();
        }

        public IEnumerable<dynamic> Run(params SqlParameter[] parameters) => DataContext.Select(Metadata, Query, parameters);

        public SelectCommandDynamic From() => From(Metadata);
        public SelectCommandDynamic From(IModelMetadata metadata) => From(objectName: metadata.FullObjectName);
        public SelectCommandDynamic From(string objectName) => From(new SqlName(objectName));
        public SelectCommandDynamic From(ISqlExpression subquery, string alias) => From(new SqlExpressionWithAlias(subquery, alias));
        public SelectCommandDynamic From(ISqlExpression objectName)
        {
            Query.From(objectName);
            return this;
        }
        public SelectCommandDynamic Limit(ISqlExpression limit)
        {
            Query.Limit(limit);
            return this;
        }

        public SelectCommandDynamic Offset(ISqlExpression offset)
        {
            Query.Offset(offset);
            return this;
        }
        public SelectCommandDynamic OrderBy(params string[] columnNames)
            => (columnNames == null || columnNames.Length == 0
            ? this
            : OrderBy(columnNames.Select(cn => new SqlOrderByClause(new SqlName(cn))).ToArray()));

        public SelectCommandDynamic OrderBy(params ISqlExpression[] custom)
            => (custom == null || custom.Length == 0
            ? this
            : OrderBy(custom.Select(cn => new SqlOrderByClause(cn)).ToArray()));
        public SelectCommandDynamic OrderBy(params SqlOrderByClause[] order)
        {
            Query.OrderBy(order);
            return this;
        }
        public SelectCommandDynamic Select() => Select(Metadata);
        public SelectCommandDynamic Select(IModelMetadata modelMetadata) => Select(modelMetadata.GetColumnsForSelect().Select(colName => new SqlName(colName)).ToArray());
        public SelectCommandDynamic Select(params string[] selects) => Select(selects.Select(s => new SqlCustom(s)).ToArray());
        public SelectCommandDynamic Select(params ISqlExpression[] selects)
        {
            Query.Select(selects);
            return this;
        }
        public SelectCommandDynamic Where(string columnName, object value) => Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));
        public SelectCommandDynamic Where(dynamic model) => Where(DynamicMapper.GetMapper(Metadata).GetWhereClause(model));
        public SelectCommandDynamic Where(SqlWhere where)
        {
            Query.Where(where);
            return this;
        }
    }
}

