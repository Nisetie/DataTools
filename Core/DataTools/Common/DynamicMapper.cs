using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataTools.Common
{
    public class DynamicMapper
    {
        private static Dictionary<string, DynamicMapper> _mappers = new Dictionary<string, DynamicMapper>();
        public static DynamicMapper GetMapper(IModelMetadata modelMetadata)
        {
            if (!_mappers.TryGetValue(modelMetadata.ModelName, out var mapper))
                _mappers[modelMetadata.ModelName] = mapper = new DynamicMapper(modelMetadata);
            return mapper;
        }
        public static void RemoveMapper(IModelMetadata modelMetadata) => _mappers.Remove(modelMetadata.ModelName);
        public static void ClearMappers() => _mappers.Clear();

        public Func<dynamic, object[]> GetArrayOfValues { get; private set; }
        public Func<dynamic, SqlWhere> GetWhereClause { get; private set; }
        public Action<dynamic, IDataContext, Dictionary<Type, Func<object, object>>, object[], SelectCache> MapObjectArrayToModel { get; private set; }
        public Func<dynamic, string> GetModelKeyValue { get; private set; }

        public static ParameterExpression GetModelInputParameterExpression()
        {
            return Expression.Parameter(typeof(object), "m");
        }

        public static Expression GetModelPropertyExpression(Expression parameterExpression, string PropertyName)
        {
            return Expression.Property(Expression.Convert(parameterExpression, typeof(DynamicModel)), "Item", Expression.Constant(PropertyName));
        }

        public static Expression GetModelPropertySetterExpression(ParameterExpression modelObject, string PropertyName, Expression value)
        {
            return Expression.Assign(Expression.Property(Expression.Convert(modelObject, typeof(DynamicModel)), "Item", Expression.Constant(PropertyName)), Expression.Convert(value, typeof(object)));
        }

        public static ParameterExpression GetForeignModelCacheVariableExpression(IModelMetadata modelMetadata)
        {
            return Expression.Variable(typeof(SelectDynamicCache), $"foreignModelCache_{modelMetadata.ObjectName}");
        }

        public static ParameterExpression GetTargetModelCacheVariableExpression()
        {
            return Expression.Variable(typeof(SelectDynamicCache), $"modelCache");
        }


        public static ParameterExpression GetForeignModelVariableExpression(IModelFieldMetadata modelFieldMetadata)
        {
            return Expression.Variable(typeof(object), $"model_{modelFieldMetadata.FieldName}");
        }

        public static Expression GetCallGetModelCacheExpression(ParameterExpression param_queryCache, IModelMetadata metadata)
        {
            return Expression.Call(param_queryCache, nameof(SelectCache.GetModelCache), null, Expression.Constant(metadata));
        }

        public static Expression GetModelAssignNewExpression(IModelMetadata modelMetadata, ParameterExpression var_model)
        {
            return Expression.Assign(var_model, Expression.New(typeof(DynamicModel).GetConstructor(new Type[] { typeof(IModelMetadata) }), Expression.Constant(modelMetadata)));
        }

        public static Expression GetInvokeMapModelExpression(
            ParameterExpression var_model,
            IModelMetadata modelMetadata,
            ParameterExpression param_dataContext,
            ParameterExpression param_customTypeConverters,
            ParameterExpression var_foreignModelQueryResult,
            ParameterExpression param_queryCache
            )
        {
            return
                Expression.Invoke(
                    Expression.Property(
                        Expression.Call(null, typeof(DynamicMapper).GetMethod(nameof(DynamicMapper.GetMapper)), Expression.Constant(modelMetadata))
                        , nameof(DynamicMapper.MapObjectArrayToModel)
                        )
                    , var_model, param_dataContext, param_customTypeConverters, var_foreignModelQueryResult, param_queryCache
                );
        }

        public DynamicMapper(IModelMetadata metadata)
        {
            GetArrayOfValues = MappingHelper.PrepareGetArrayOfValuesCommand<Func<dynamic, object[]>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            GetWhereClause = MappingHelper.PrepareCreateWhereClause<Func<dynamic, SqlWhere>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            MapObjectArrayToModel = MappingHelper.PrepareMapModel<Action<dynamic, IDataContext, Dictionary<Type, Func<object, object>>, object[], SelectCache>>(
                metadata,
                GetModelInputParameterExpression,
                GetModelPropertyExpression,
                GetModelPropertySetterExpression,
                GetForeignModelCacheVariableExpression,
                GetForeignModelVariableExpression,
                GetTargetModelCacheVariableExpression,
                GetCallGetModelCacheExpression,
                GetInvokeMapModelExpression,
                GetModelAssignNewExpression
                );
            GetModelKeyValue = MappingHelper.PrepareGetModelKeyValue<Func<dynamic, string>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
        }

        public static void CopyValues(IModelMetadata modelMetadata, dynamic from, dynamic to)
        {
            foreach (var f in modelMetadata.Fields)
            {
                to[f.FieldName] = from[f.FieldName];
            }
        }
    }
}

