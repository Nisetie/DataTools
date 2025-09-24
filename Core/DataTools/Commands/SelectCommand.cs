using DataTools.Commands;
using DataTools.Common;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataTools.Commands
{
    public class SelectCommand<ModelT> : ISqlExpression, ISqlSelect<SelectCommand<ModelT>> where ModelT : class, new()
    {
        public IDataContext DataContext { get; }
        public SqlSelect Query { get; }
        public ISqlExpression FromSource => Query.FromSource;
        public ISqlExpression LimitRows => Query.LimitRows;
        public ISqlExpression OffsetRows => Query.OffsetRows;
        public IEnumerable<SqlOrderByClause> Orders => Query.Orders;
        public IEnumerable<ISqlExpression> Selects => Query.Selects;
        public SqlWhere Wheres => Query.Wheres;
        public SelectCommand(IDataContext dataContext)
        {
            DataContext = dataContext;
            Query = new SqlSelect();
        }

        public IEnumerable<ModelT> Run(params SqlParameter[] parameters) => DataContext.Select<ModelT>(Query, parameters);

        public SelectCommand<ModelT> From() => From(ModelMetadata<ModelT>.Instance);
        public SelectCommand<ModelT> From(IModelMetadata metadata) => From(objectName: metadata.FullObjectName);
        public SelectCommand<ModelT> From(string objectName) => From(new SqlName(objectName));
        public SelectCommand<ModelT> From(ISqlExpression subquery, string alias) => From(new SqlExpressionWithAlias(subquery, alias));
        public SelectCommand<ModelT> From(ISqlExpression objectName)
        {
            Query.From(objectName);
            return this;
        }
        public SelectCommand<ModelT> Limit(ISqlExpression limit)
        {
            Query.Limit(limit);
            return this;
        }

        public SelectCommand<ModelT> Offset(ISqlExpression offset)
        {
            Query.Offset(offset);
            return this;
        }
        public SelectCommand<ModelT> OrderBy(params string[] columnNames)
            => (columnNames == null || columnNames.Length == 0
            ? this
            : OrderBy(columnNames.Select(cn => new SqlOrderByClause(new SqlName(cn))).ToArray()));

        public SelectCommand<ModelT> OrderBy(params ISqlExpression[] custom)
            => (custom == null || custom.Length == 0
            ? this
            : OrderBy(custom.Select(cn => new SqlOrderByClause(cn)).ToArray()));
        public SelectCommand<ModelT> OrderBy(params SqlOrderByClause[] order)
        {
            Query.OrderBy(order);
            return this;
        }
        public SelectCommand<ModelT> Select() => Select(ModelMetadata<ModelT>.Instance);
        public SelectCommand<ModelT> Select(IModelMetadata modelMetadata) => Select(modelMetadata.GetColumnsForSelect().Select(colName => new SqlName(colName)).ToArray());
        public SelectCommand<ModelT> Select(params string[] selects) => Select(selects.Select(s => new SqlCustom(s)).ToArray());
        public SelectCommand<ModelT> Select(params ISqlExpression[] selects)
        {
            Query.Select(selects);
            return this;
        }

        public SelectCommand<ModelT> Where
            (string columnName, object value) 
            => value == null 
            ? Where(new SqlWhere(new SqlName(columnName)).IsNull()) 
            : Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));
        public SelectCommand<ModelT> Where(ModelT model) => Where(ModelMapper<ModelT>.GetWhereClause(model));
        public SelectCommand<ModelT> Where(Expression<Func<ModelT, bool>> filterExpression)
        {
            return Where(new SqlWhere(SelectCommandHelper.ProcessExpression(ModelMetadata<ModelT>.Instance, filterExpression.Body)));
        }
        public SelectCommand<ModelT> Where(SqlWhere where)
        {
            Query.Where(where);
            return this;
        }
    }
}

