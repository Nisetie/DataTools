using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Dynamic;
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

        public Action<SqlInsert, dynamic> BindInsertValues { get; private set; }
        public Action<SqlUpdate, dynamic> BindUpdateValues { get; private set; }
        public Action<SqlUpdate, dynamic> BindUpdateWhere { get; private set; }
        public Action<SqlDelete, dynamic> BindDeleteWhere { get; private set; }
        public Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], SelectCache, dynamic> MapModel { get; private set; }
        public Func<dynamic, string> GetModelKeyValue { get; private set; }

        public static ParameterExpression GetModelInputParameterExpression()
        {
            return Expression.Parameter(typeof(object), "m");
        }

        public static ParameterExpression GetModelVariableExpression()
        {
            return Expression.Variable(typeof(object), "m");
        }

        public static Expression GetModelPropertyExpression(Expression parameterExpression, string PropertyName)
        {
            return Expression.Property(Expression.Convert(parameterExpression, typeof(DynamicModel)), "Item", Expression.Constant(PropertyName));
        }

        public static Expression GetModelPropertySetterExpression(ParameterExpression modelObject, string PropertyName, Expression value)
        {
            return Expression.Assign(Expression.Property(Expression.Convert(modelObject, typeof(DynamicModel)), "Item", Expression.Constant(PropertyName)), Expression.Convert( value,typeof(object)));
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

        public static Expression GetLocalModelAssignNewExpression(IModelMetadata modelMetadata, ParameterExpression var_m)
        {
            return Expression.Assign(var_m, Expression.New(typeof(DynamicModel).GetConstructor(new Type[] { typeof(IModelMetadata) }), Expression.Constant(modelMetadata)));
        }

        public static Expression GetInvokeMapModelExpression(
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
                        , nameof(DynamicMapper.MapModel)
                        )
                    , param_dataContext, param_customTypeConverters, var_foreignModelQueryResult, param_queryCache
                );
        }

        public DynamicMapper(IModelMetadata metadata)
        {
            BindInsertValues = MappingHelper.PrepareInsertCommand<Action<SqlInsert, dynamic>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            BindUpdateValues = MappingHelper.PrepareBindUpdateValuesCommand<Action<SqlUpdate, dynamic>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            BindUpdateWhere = MappingHelper.PrepareBindUpdateWhereCommand<Action<SqlUpdate, dynamic>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            BindDeleteWhere = MappingHelper.PrepareDeleteWhereCommand<Action<SqlDelete, dynamic>>(metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            MapModel = MappingHelper.PrepareMapModel<Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], SelectCache, dynamic>>(
                metadata,
                GetModelInputParameterExpression,
                GetModelPropertySetterExpression,
                GetForeignModelCacheVariableExpression,
                GetForeignModelVariableExpression,
                GetTargetModelCacheVariableExpression,
                GetCallGetModelCacheExpression,
                GetInvokeMapModelExpression,
                GetLocalModelAssignNewExpression
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

