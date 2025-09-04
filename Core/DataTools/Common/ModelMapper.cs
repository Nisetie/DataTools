using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataTools.Common
{
    /// <summary>
    /// Класс для хранения логики преобразования данных в модель и обратно.
    /// </summary>
    /// <typeparam name="ModelT">Тип данных целевой модели</typeparam>
    public static class ModelMapper<ModelT> where ModelT : class, new()
    {
        private static IModelMetadata Metadata = ModelMetadata<ModelT>.Instance;
        private static Action<ModelT, ModelT> _modelCopier;
        public static Action<SqlInsert, ModelT> BindInsertValues { get; private set; }

        /// <summary>
        /// Смаппировать модель с массивом значений.
        /// Параметры функции:
        /// контекст данных (IDataContext);
        /// произвольные преобразователи типов (Dictionary<Type, Func<object, object>>);
        /// строка данных из источника (object[]);
        /// кеш моделей (QueryCache).
        /// </summary>
        public static Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], SelectCache, ModelT> MapModel { get; private set; }

        public static Action<SqlUpdate, ModelT> BindUpdateValues { get; private set; }
        public static Action<SqlUpdate, ModelT> BindUpdateWhere { get; private set; }
        public static Action<SqlDelete, ModelT> BindDeleteWhere { get; private set; }

        public static Func<ModelT, string> GetModelKeyValue { get; private set; }

        public static ParameterExpression GetModelInputParameterExpression()
        {
            return Expression.Parameter(typeof(ModelT), "m");
        }

        public static ParameterExpression GetModelVariableExpression()
        {
            return Expression.Variable(typeof(ModelT), "m");
        }

        public static Expression GetModelPropertyExpression(Expression parameterExpression, string PropertyName)
        {
            return Expression.Property(parameterExpression, PropertyName);
        }

        public static Expression GetModelPropertySetterExpression(ParameterExpression modelObject, string PropertyName, Expression value)
        {
            return Expression.Assign(Expression.Property(modelObject, PropertyName), value);
        }

        public static ParameterExpression GetForeignModelCacheVariableExpression(IModelMetadata modelMetadata)
        {
            return Expression.Variable(typeof(SelectModelCache<>).MakeGenericType(Type.GetType(modelMetadata.ModelTypeName)), $"foreignModelCache_{modelMetadata.ObjectName}");
        }

        public static ParameterExpression GetTargetModelCacheVariableExpression()
        {
            return Expression.Variable(typeof(SelectModelCache<>).MakeGenericType(typeof(ModelT)), $"modelCache");
        }

        public static ParameterExpression GetForeignModelVariableExpression(IModelFieldMetadata modelFieldMetadata)
        {
            return Expression.Variable(Type.GetType(modelFieldMetadata.ForeignModel.ModelTypeName), $"model_{modelFieldMetadata.FieldName}");
        }

        public static Expression GetCallGetModelCacheExpression(ParameterExpression param_queryCache, IModelMetadata metadata)
        {
            return Expression.Call(param_queryCache, nameof(SelectCache.GetModelCache), typeArguments: new Type[] { Type.GetType(metadata.ModelTypeName) }, null);
        }

        public static Expression GetLocalModelAssignNewExpression(IModelMetadata modelMetadata, ParameterExpression var_m)
        {
            return Expression.Assign(var_m, Expression.New(typeof(ModelT)));
        }

        public static Expression GetInvokeMapModelExpression(
          IModelMetadata modelMetadata,
          ParameterExpression param_dataContext,
          ParameterExpression param_customTypeConverters,
          ParameterExpression var_foreignModelQueryResult,
          ParameterExpression param_queryCache
          )
        {
            return Expression.Invoke(Expression.Property(null, typeof(ModelMapper<>).MakeGenericType(Type.GetType(modelMetadata.ModelTypeName)).GetProperty(nameof(MapModel))), param_dataContext, param_customTypeConverters, var_foreignModelQueryResult, param_queryCache);
        }

        private static void PrepareInsertCommand()
        {
            BindInsertValues = MappingHelper.PrepareInsertCommand<Action<SqlInsert, ModelT>>(Metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
        }

        /// <summary>
        /// Подготовка функции для маппинга только одной модели
        /// </summary>
        private static void PrepareMapModel()
        {
            MapModel = MappingHelper.PrepareMapModel<Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], SelectCache, ModelT>>(
                Metadata,
                GetModelInputParameterExpression,
                GetModelPropertySetterExpression,
                GetForeignModelCacheVariableExpression,
                GetForeignModelVariableExpression,
                GetTargetModelCacheVariableExpression,
                GetCallGetModelCacheExpression,
                GetInvokeMapModelExpression,
                GetLocalModelAssignNewExpression
                );
        }

        /// <summary>
        /// Подготовка команды для обновления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда обновление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// Необходимо вручную описать фильтрацию WHERE.
        /// </summary>
        private static void PrepareUpdateCommand()
        {
            BindUpdateValues = MappingHelper.PrepareBindUpdateValuesCommand<Action<SqlUpdate, ModelT>>(Metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
            BindUpdateWhere = MappingHelper.PrepareBindUpdateWhereCommand<Action<SqlUpdate, ModelT>>(Metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
        }

        /// <summary>
        /// Подготовка команды для удаления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда удаление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// Необходимо вручную описать фильтрацию WHERE.
        /// </summary>
        private static void PrepareDeleteCommand()
        {
            BindDeleteWhere = MappingHelper.PrepareDeleteWhereCommand<Action<SqlDelete, ModelT>>(Metadata, GetModelInputParameterExpression, GetModelPropertyExpression);
        }

        private static void PrepareModelCopier()
        {
            var param_from = Expression.Parameter(typeof(ModelT));
            var param_to = Expression.Parameter(typeof(ModelT));

            List<BinaryExpression> assigns = new List<BinaryExpression>();
            foreach (var f in Metadata.Fields)
            {
                assigns.Add(Expression.Assign(Expression.Property(param_to, f.FieldName), Expression.Property(param_from, f.FieldName)));
            }

            _modelCopier = Expression.Lambda<Action<ModelT, ModelT>>(Expression.Block(assigns), param_from, param_to).Compile();
        }

        static ModelMapper()
        {
            PrepareInsertCommand();
            PrepareMapModel();
            PrepareUpdateCommand();
            PrepareDeleteCommand();
            GetModelKeyValue = MappingHelper.PrepareGetModelKeyValue<Func<ModelT, string>>(ModelMetadata<ModelT>.Instance, GetModelInputParameterExpression, GetModelPropertyExpression);
            PrepareModelCopier();
        }

        public static void CopyValues(ModelT from, ModelT to)
        {
            _modelCopier(from, to);
        }
    }
}