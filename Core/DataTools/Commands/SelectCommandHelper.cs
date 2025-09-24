using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System;
using System.Linq.Expressions;

namespace DataTools.Commands
{
    public static class SelectCommandHelper
    {
        public static ISqlExpression ProcessExpression(IModelMetadata modelMetadata, Expression expression)
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

        public static ISqlExpression ProcessInvocationExpression(IModelMetadata modelMetadata, InvocationExpression invocationExpression)
        {
            return new SqlConstant(Expression.Lambda(invocationExpression).Compile().DynamicInvoke());
        }

        public static ISqlExpression ProcessMemberExpression(IModelMetadata modelMetadata, MemberExpression memberExpression)
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

        public static ISqlExpression ProcessUnaryExpression(IModelMetadata modelMetadata, UnaryExpression unaryExpression)
        {
            if (unaryExpression.NodeType == ExpressionType.Not)
                return new SqlWhere().Not(ProcessExpression(modelMetadata, unaryExpression.Operand));
            if (unaryExpression.NodeType == ExpressionType.Convert)
                return ProcessExpression(modelMetadata, unaryExpression.Operand);
            else
                return ProcessExpression(modelMetadata, unaryExpression.Operand);
            throw new NotImplementedException();
        }

        public static ISqlExpression ProcessBinaryExpression(IModelMetadata modelMetadata, BinaryExpression binaryExpression)
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