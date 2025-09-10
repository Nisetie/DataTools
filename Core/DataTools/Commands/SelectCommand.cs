using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
namespace DataTools.Commands
{
    public interface ISelectCommand<SelectCommandT>
    {
        IDataContext Context { get; set; }
        IModelMetadata Metadata { get; set; }
        SqlSelect Query { get; set; }

        SelectCommandT Limit(SqlExpression limit);
        SelectCommandT Offset(SqlExpression offset);
        SelectCommandT OrderBy(params string[] columnNames);
        SelectCommandT Where(SqlWhere whereClause);
        SelectCommandT Where(string columnName, object value);
    }

    public interface ISelectCommandDynamicResult
    {
        IEnumerable<dynamic> Select();
        IEnumerable<dynamic> Select(params SqlParameter[] parameters);
    }

    public interface ISelectCommandExactResult<ModelT> where ModelT : class, new()
    {
        IEnumerable<ModelT> Select();

        IEnumerable<ModelT> Select(params SqlParameter[] parameters);
    }

    public abstract class SelectCommandBase<SelectCommandT> : ISelectCommand<SelectCommandT> where SelectCommandT : SelectCommandBase<SelectCommandT>
    {
        public IDataContext Context { get; set; }
        public IModelMetadata Metadata { get; set; }
        public SqlSelect Query { get; set; }

        public SelectCommandBase(IDataContext context, IModelMetadata metadata)
        {
            Context = context;
            Metadata = metadata;
            Query = new SqlSelect().From(metadata);
        }

        public SelectCommandT Where(string columnName, object value)
        {
            if (value == null)
                Query.Where(new SqlWhere(new SqlName(columnName)).IsNull());
            else
                Query.Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));
            return this as SelectCommandT;
        }

        public SelectCommandT Where(SqlWhere whereClause)
        {
            Query.Where(whereClause);
            return this as SelectCommandT;
        }

        public SelectCommandT OrderBy(params string[] columnNames)
        {
            var orders = new SqlOrderByClause[columnNames.Length];
            for (int i = 0; i < orders.Length; i++)
                orders[i] = new SqlOrderByClause(new SqlName(columnNames[i]));
            Query.OrderBy(orders);
            return this as SelectCommandT;
        }

        public SelectCommandT Offset(SqlExpression offset)
        {
            Query.Offset(offset);
            return this as SelectCommandT;
        }
        public SelectCommandT Limit(SqlExpression limit)
        {
            Query.Limit(limit);
            return this as SelectCommandT;
        }

        public override string ToString()
        {
            return Query.ToString();
        }
    }

    public class SelectCommand : SelectCommandBase<SelectCommand>, ISelectCommandDynamicResult
    {
        public SelectCommand(IDataContext context, IModelMetadata metadata) : base(context, metadata)
        {
        }

        public IEnumerable<dynamic> Select() => Context.Select(Metadata, Query);
        public IEnumerable<dynamic> Select(params SqlParameter[] parameters) => Context.Select(Metadata, Query, parameters);
    }

    public class SelectCommand<ModelT> : SelectCommandBase<SelectCommand<ModelT>>, ISelectCommandExactResult<ModelT> where ModelT : class, new()
    {
        public SelectCommand(IDataContext context) : base(context, ModelMetadata<ModelT>.Instance) { }

        public IEnumerable<ModelT> Select() => Context.Select<ModelT>(Query);

        public IEnumerable<ModelT> Select(params SqlParameter[] parameters) => Context.Select<ModelT>(Query, parameters);

        public SelectCommand<ModelT> Where(Expression<Func<ModelT, bool>> filterExpression)
        {
            return this.Where(new SqlWhere(SelectCommandHelper.ProcessExpression(Metadata, filterExpression.Body)));
        }
    }

    public static class SelectCommandHelper
    {
        public static SqlExpression ProcessExpression(IModelMetadata modelMetadata, Expression expression)
        {
            var where = new SqlWhere();

            switch (expression)
            {
                case BinaryExpression binaryExpression:
                    return new SqlWhere(ProcessBinaryExpression(modelMetadata, binaryExpression));
                case UnaryExpression unaryExpression:
                    return ProcessUnaryExpression(modelMetadata, unaryExpression);
                case MemberExpression memberExpression:
                    return ProcessMemberExpression(modelMetadata, memberExpression);
                case InvocationExpression invocationExpression:
                    return ProcessInvocationExpression(modelMetadata, invocationExpression);
                case ConstantExpression constantExpression:
                    if (constantExpression.Value != null)
                        return new SqlConstant(constantExpression.Value);
                    else
                        return new SqlIsNull();
            }
            return where;
        }

        public static SqlExpression ProcessInvocationExpression(IModelMetadata modelMetadata, InvocationExpression invocationExpression)
        {
            return new SqlConstant(Expression.Lambda(invocationExpression).Compile().DynamicInvoke());
        }

        public static SqlExpression ProcessMemberExpression(IModelMetadata modelMetadata, MemberExpression memberExpression)
        {
            var modelType = memberExpression.Expression.Type;
            if (modelType == Type.GetType(modelMetadata.ModelTypeName))
            {
                return new SqlName(modelMetadata.GetField(memberExpression.Member.Name).ColumnName);
            }
            else
            {
                return new SqlConstant(Expression.Lambda(memberExpression).Compile().DynamicInvoke());
            }
        }

        public static SqlExpression ProcessUnaryExpression(IModelMetadata modelMetadata, UnaryExpression unaryExpression)
        {
            if (unaryExpression.NodeType == ExpressionType.Not)
                return new SqlWhere().Not(ProcessExpression(modelMetadata, unaryExpression.Operand));
            if (unaryExpression.NodeType == ExpressionType.Convert)
                return ProcessExpression(modelMetadata, unaryExpression.Operand);
            throw new NotImplementedException();
        }

        public static SqlExpression ProcessBinaryExpression(IModelMetadata modelMetadata, BinaryExpression binaryExpression)
        {
            var left = binaryExpression.Left;
            var right = binaryExpression.Right;

            switch (binaryExpression.NodeType)
            {
                case ExpressionType.AndAlso:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).And(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.OrElse:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Or(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.Equal:
                    var l = ProcessExpression(modelMetadata, binaryExpression.Left);
                    var r = ProcessExpression(modelMetadata, binaryExpression.Right);
                    if (r is SqlIsNull)
                        return new SqlWhere(l).IsNull();
                    else
                        return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Eq(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.GreaterThan:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Gt(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.GreaterThanOrEqual:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Ge(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.LessThan:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Lt(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.LessThanOrEqual:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Le(ProcessExpression(modelMetadata, binaryExpression.Right));
                case ExpressionType.NotEqual:
                    return new SqlWhere(ProcessExpression(modelMetadata, binaryExpression.Left)).Ne(ProcessExpression(modelMetadata, binaryExpression.Right));
                default:
                    throw new NotImplementedException();
            }
        }
    }
}