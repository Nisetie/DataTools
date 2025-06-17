using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataTools.Common
{
    /// <summary>
    /// Промежуточное хранилище сущностей, запрашиваемых по внешним ключам.
    /// Используется только для команды SELECT.
    /// </summary>
    public class QueryCache
    {
        private Dictionary<string, ModelCache> _caches = new Dictionary<string, ModelCache>();

        public ModelCache<ModelT> GetModelCache<ModelT>() where ModelT : class, new()
        {
            string modelName = ModelCache<ModelT>.ModelName;
            if (!_caches.TryGetValue(modelName, out var cache))
            {
                _caches[modelName] = cache = new ModelCache<ModelT>();
                return cache as ModelCache<ModelT>;
            }
            return cache as ModelCache<ModelT>;
        }
    }

    public abstract class ModelCache
    {

    }

    /// <summary>
    /// Промежуточный кеш сущности. Кеш хранит словарь сущностей "ключ-сущность". Где ключом является строковая конкатенация ключевых полей.
    /// Используется только для команды SELECT.
    /// </summary>
    /// <typeparam name="ModelT"></typeparam>
    public class ModelCache<ModelT> : ModelCache
        where ModelT : class, new()
    {
        public static readonly string ModelName;

        /// <summary>
        /// Первичный ключ преобразуется в строку для универсальности.
        /// Предполагается, что первичный ключ имеет простой тип данных (легко переводить в строку и обратно)
        /// </summary>
        public Dictionary<object, ModelT> uniqueKeys = new Dictionary<object, ModelT>();
        public SqlSelect CachedSelect = new SqlSelect().From<ModelT>();

        static ModelCache()
        {
            ModelName = ModelMetadata<ModelT>.Instance.FullObjectName;

            var primaryKeys = (from f in ModelMetadata<ModelT>.Instance.Fields
                               where f.IsAutoincrement || f.IsUnique
                               select f).ToArray();


            if (primaryKeys.Length != 1)
            {
                throw new Exception($"{nameof(ModelCache<ModelT>)}: primaryKeys.Length is {primaryKeys.Length}! Must be one.");
            }
        }

        public bool TryGetModelByKey(object key, out ModelT model)
        {
            return uniqueKeys.TryGetValue(key, out model);
        }

        public void AddModelWithKey(object key, ModelT model)
        {
            if (!uniqueKeys.ContainsKey(key))
                uniqueKeys[key] = model;
        }
    }

    /// <summary>
    /// Класс для описания логики преобразования данных в модель и обратно.
    /// </summary>
    /// <typeparam name="ModelT">Тип данных целевой модели</typeparam>
    public static class ModelMapper<ModelT> where ModelT : class, new()
    {
        private static IModelMetadata Metadata = ModelMetadata<ModelT>.Instance;

        private static ParameterExpression param_dataContext = Expression.Parameter(typeof(IDataContext), "dataContext");
        private static ParameterExpression param_customTypeConverters = Expression.Parameter(typeof(Dictionary<Type, Func<object, object>>), "customConverters");
        private static ParameterExpression param_dataRow = Expression.Parameter(typeof(object[]), "dataRow");
        private static ParameterExpression param_dataRows = Expression.Parameter(typeof(IEnumerable<object[]>), "dataRows");

        private static ParameterExpression param_insertBuilder = Expression.Parameter(typeof(SqlInsert), "insertBuilder");
        private static ParameterExpression param_m = Expression.Parameter(Metadata.ModelType, "m");

        private static ParameterExpression var_selectBuilder = Expression.Variable(typeof(SqlSelect));
        private static ParameterExpression var_m = Expression.Variable(Metadata.ModelType, "m");
        private static ParameterExpression var_value = Expression.Variable(typeof(object), "value");
        private static ParameterExpression var_customConverter = Expression.Variable(typeof(Func<object, object>), "customConverter");
        private static ParameterExpression param_queryCache = Expression.Parameter(typeof(QueryCache), "queryCache");
        private static ParameterExpression var_foreignModelQueryResult = Expression.Variable(typeof(object[]), "foreignModelQueryResult");


        public static Action<SqlInsert, ModelT> BindInsertValues { get; private set; }

        /// <summary>
        /// Смаппировать модель с массивом значений.
        /// Параметры функции:
        /// контекст данных (IDataContext);
        /// произвольные преобразователи типов (Dictionary<Type, Func<object, object>>);
        /// строка данных из источника (object[]);
        /// кеш моделей (QueryCache).
        /// </summary>
        public static Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], QueryCache, ModelT> MapModel { get; private set; }

        public static Action<SqlUpdate, ModelT> BindUpdateValues { get; private set; }
        public static Action<SqlDelete, ModelT> BindDeleteValues { get; private set; }

        private static IEnumerable<IModelFieldMetadata> GetChangeableFields()
        {
            return from field
                   in Metadata.Fields
                   where !(field.IgnoreChanges || field.IsAutoincrement)
                   orderby field.FieldOrder
                   select field;
        }

        private static void PrepareInsertCommand()
        {
            var call = Expression.Call(
                typeof(SqlInsertExtensions),
                nameof(SqlInsertExtensions.Value),
                typeArguments: null,
                param_insertBuilder,
                    // new object[]
                    Expression.NewArrayInit(
                        typeof(object),
                        (from field
                         in GetChangeableFields()
                         select (field.IsForeignKey == false
                         ? Expression.Convert(Expression.Property(param_m, field.ColumnName), typeof(object)) as Expression
                         : Expression.Condition(
                             Expression.NotEqual(Expression.Property(param_m, field.FieldName), Expression.Convert(Expression.Constant(null), field.FieldType))
                             , Expression.Convert(Expression.Property(Expression.Property(param_m, field.FieldName), field.ForeignColumnName), typeof(object))
                             , Expression.Convert(Expression.Constant(null), typeof(object)))
                         )).ToArray()
                         )
                );

            var lambda = Expression.Lambda<Action<SqlInsert, ModelT>>(call, param_insertBuilder, param_m);
            BindInsertValues = lambda.Compile();
        }

        /// <summary>
        /// Подготовка функции для маппинга только одной модели
        /// </summary>
        private static void PrepareMapModel()
        {
            var fields = from f in Metadata.Fields orderby f.FieldOrder select f;

            // создать кеширующие временные словари для связанных моделей
            var foreignModelCaches = new Dictionary<string, ParameterExpression>();
            var foreignModelVariables = new Dictionary<string, ParameterExpression>();
            var foreignModelKeys = new Dictionary<string, ParameterExpression>();
            var foreignModelSelects = new Dictionary<string, ParameterExpression>();
            foreach (var f in fields)
            {
                if (f.IsForeignKey == false) continue;
                foreignModelCaches[f.FieldName] = Expression.Variable(typeof(ModelCache<>).MakeGenericType(f.FieldType), $"foreignModelCache_{f.FieldName}");
                foreignModelVariables[f.FieldName] = Expression.Variable(f.ForeignModel.ModelType, $"model{f.FieldName}");
                foreignModelKeys[f.FieldName] = Expression.Variable(f.ForeignModel.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType, $"modelKey{f.FieldName}");
                foreignModelSelects[f.FieldName] = Expression.Variable(typeof(SqlSelect), $"select{f.FieldName}");
            }

            var all_variables = new ParameterExpression[] { } as IEnumerable<ParameterExpression>;
            all_variables = all_variables.Concat(foreignModelCaches.Select(kv => kv.Value));
            all_variables = all_variables.Concat(foreignModelVariables.Select(kv => kv.Value));
            all_variables = all_variables.Concat(foreignModelKeys.Select(kv => kv.Value));
            all_variables = all_variables.Concat(foreignModelSelects.Select(kv => kv.Value));
            all_variables = all_variables.Concat(new[] { var_m, var_value, var_customConverter, var_foreignModelQueryResult });

            var finalBlock =
                   Expression.Block(
                       Expression.Empty()
                       , Expression.Assign(var_m, Expression.New(typeof(ModelT)))
                       , Expression.Block(
                           from f
                           in fields
                               // для поля с простым типом данных
                           let fieldType = !f.IsForeignKey ? f.FieldType : f.ForeignModel.Fields.First(ff => ff.ColumnName == f.ForeignColumnName).FieldType
                           let UnboxedType = Nullable.GetUnderlyingType(fieldType)
                           let IsNullable = UnboxedType != null
                           let RealType = IsNullable ? UnboxedType : fieldType
                           let IsConvertible = RealType.GetInterface(nameof(IConvertible)) != null
                           let ParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string) })
                           let IsParsable = ParseMethod != null
                           let ToStringMethod = typeof(object).GetMethod(nameof(ToString), new Type[] { })
                           // для поля - внешней модели
                           let var_foreignModelCache = f.IsForeignKey ? foreignModelCaches[f.FieldName] : null
                           let var_foreignModel = f.IsForeignKey ? foreignModelVariables[f.FieldName] : null
                           let var_foreignModelSelect = f.IsForeignKey ? foreignModelSelects[f.FieldName] : null
                           let var_foreignKeyValue = f.IsForeignKey ? foreignModelKeys[f.FieldName] : null

                           select Expression.Block(
                               Expression.Assign(var_value, Expression.ArrayIndex(param_dataRow, Expression.Constant(f.FieldOrder)))
                               , f.IsForeignKey == false
                                // свойства, имеющие примитивные типы
                                ? Expression.IfThenElse(
                                    Expression.Equal(var_value, Expression.Constant(null))
                                    , Expression.Assign(Expression.Property(var_m, f.FieldName), Expression.Default(fieldType))
                                   , Expression.Assign(
                                       Expression.Property(var_m, f.FieldName),
                                        IsConvertible
                                        ? Expression.Convert(Expression.Call(typeof(Convert), "ChangeType", null, var_value, Expression.Constant(RealType)), fieldType)
                                        : IsParsable
                                        ? Expression.Convert(Expression.Call(ParseMethod, Expression.Call(var_value, "ToString", null, null)), fieldType)
                                        : Expression.Convert(var_value, fieldType)
                                        )
                                   )
                               : Expression.IfThen(
                                   Expression.NotEqual(var_value, Expression.Constant(null)),
                                   Expression.Block(
                                       Expression.Assign(
                                           var_foreignKeyValue,
                                           IsConvertible
                                        ? Expression.Convert(Expression.Call(typeof(Convert), "ChangeType", null, var_value, Expression.Constant(RealType)), fieldType)
                                        : IsParsable
                                        ? Expression.Convert(Expression.Call(ParseMethod, Expression.Call(var_value, "ToString", null, null)), fieldType)
                                        : Expression.Convert(var_value, fieldType)
                                           )
                                       , Expression.Assign(var_foreignModelCache, Expression.Call(param_queryCache, nameof(QueryCache.GetModelCache), typeArguments: new Type[] { f.FieldType }, null))
                                       , Expression.IfThenElse(
                                           Expression.Call(var_foreignModelCache, nameof(ModelCache<ModelT>.TryGetModelByKey), null, Expression.Convert(var_foreignKeyValue, typeof(object)), var_foreignModel)
                                           , Expression.Assign(Expression.Property(var_m, f.FieldName), var_foreignModel)
                                           , Expression.Block(
                                               variables: null
                                               , Expression.Assign(var_foreignModelSelect, Expression.Field(var_foreignModelCache, nameof(ModelCache<ModelT>.CachedSelect)))
                                               , Expression.Call(typeof(SqlSelectExtensions), nameof(SqlSelectExtensions.Where), null, var_foreignModelSelect, Expression.Constant(f.ForeignColumnName), Expression.Convert(var_foreignKeyValue, typeof(object)))
                                               , Expression.Assign(
                                                   var_foreignModelQueryResult
                                                   , Expression.Convert(Expression.Call(typeof(Enumerable), "FirstOrDefault", new Type[] { typeof(object[]) }, Expression.Call(param_dataContext, nameof(IDataContext.ExecuteWithResult), null, var_foreignModelSelect)), typeof(object[]))
                                                   )
                                               , Expression.IfThen(
                                                   Expression.NotEqual(var_foreignModelQueryResult, Expression.Constant(null))
                                                   , Expression.Block(
                                                       Expression.Assign(var_foreignModel, Expression.Invoke(Expression.Property(null, typeof(ModelMapper<>).MakeGenericType(f.FieldType).GetProperty(nameof(MapModel))), param_dataContext, param_customTypeConverters, var_foreignModelQueryResult, param_queryCache))
                                                       , Expression.Call(var_foreignModelCache, nameof(ModelCache<ModelT>.AddModelWithKey), null, Expression.Convert(var_foreignKeyValue, typeof(object)), var_foreignModel)
                                                       , Expression.Assign(Expression.Property(var_m, f.FieldName), var_foreignModel)
                                                   )
                                               )
                                           )
                                       )
                                       )
                                   )
                               )
                           )
                       , var_m
                       );

            var all_script = Expression.Block(
                all_variables
                , finalBlock
                );

            MapModel =
                Expression.Lambda<Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], QueryCache, ModelT>>(
                all_script,
                param_dataContext,
                param_customTypeConverters,
                param_dataRow,
                param_queryCache
                ).Compile();
        }

        /// <summary>
        /// Подготовка команды для обновления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда обновление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// </summary>
        private static void PrepareUpdateCommand()
        {
            var param_updateBuilder = Expression.Parameter(typeof(SqlUpdate), "updateBuilder");
            var param_m = Expression.Parameter(Metadata.ModelType, "m");

            var var_where = Expression.Variable(typeof(SqlWhereClause));

            var all_scripts = Expression.Block(
                variables: new ParameterExpression[] { var_where }
                , Expression.Block(Expression.Call(
                    typeof(SqlUpdateExtensions),
                    nameof(SqlUpdateExtensions.Value),
                    null,
                    param_updateBuilder,
                    Expression.NewArrayInit(// new object[]
                        typeof(object),
                        (from field in Metadata.Fields
                         where !(field.IgnoreChanges || field.IsAutoincrement)
                         select
                         !field.IsForeignKey
                         ? Expression.Convert(Expression.Property(param_m, field.ColumnName), typeof(object)) as Expression
                         : Expression.Condition(
                             Expression.NotEqual(
                                 Expression.Property(param_m, field.FieldName),
                                 Expression.Convert(Expression.Constant(null), field.FieldType)
                                 ),
                             Expression.Convert(
                                 Expression.Property(Expression.Property(param_m, field.FieldName), field.ForeignColumnName),
                                 typeof(object)
                                 ),
                             Expression.Convert(Expression.Constant(null), typeof(object))
                             )
                         ).ToArray()
                         )
                    )
                )
                , Expression.Assign(var_where, Expression.New(typeof(SqlWhereClause)))
                , Expression.Call(typeof(SqlWhereClauseExtensions), nameof(SqlWhereClauseExtensions.Value), null, var_where, Expression.Constant(1, typeof(object)))
                , Expression.Call(typeof(SqlWhereClauseExtensions), nameof(SqlWhereClauseExtensions.EqValue), null, var_where, Expression.Constant(1, typeof(object)))
                , Expression.Block(
                    from f
                    in Metadata.Fields
                    where f.IsUnique == true
                    select Expression.Block(
                        Expression.Assign(
                            var_where
                            , Expression.Call(
                                typeof(SqlWhereClauseExtensions)
                                , nameof(SqlWhereClauseExtensions.AndName)
                                , null
                                , var_where
                                , Expression.Constant(f.ColumnName))
                            )
                        , Expression.Assign(
                            var_where
                            , Expression.Call(
                            typeof(SqlWhereClauseExtensions)
                            , nameof(SqlWhereClauseExtensions.EqValue)
                            , null
                            , var_where
                            , Expression.Convert(Expression.Property(param_m, f.FieldName), typeof(object))))
                        )
                    )
                , Expression.Call(param_updateBuilder, nameof(SqlUpdate.Where), null, var_where)
                );

            BindUpdateValues = Expression.Lambda<Action<SqlUpdate, ModelT>>(all_scripts, param_updateBuilder, param_m).Compile();
        }

        /// <summary>
        /// Подготовка команды для удаления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда удаление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// </summary>
        private static void PrepareDeleteCommand()
        {
            var param_deleteBuilder = Expression.Parameter(typeof(SqlDelete), "deleteBuilder");
            var param_m = Expression.Parameter(Metadata.ModelType, "m");

            var var_where = Expression.Variable(typeof(SqlWhereClause));

            var all_scripts = Expression.Block(
               variables: new ParameterExpression[] { var_where }
               , Expression.Assign(var_where, Expression.New(typeof(SqlWhereClause)))
               , Expression.Call(typeof(SqlWhereClauseExtensions), nameof(SqlWhereClauseExtensions.Value), null, var_where, Expression.Constant(1, typeof(object)))
               , Expression.Call(typeof(SqlWhereClauseExtensions), nameof(SqlWhereClauseExtensions.EqValue), null, var_where, Expression.Constant(1, typeof(object)))
               , Expression.Block(
                   from f
                   in Metadata.Fields
                   where f.IsUnique == true
                   select Expression.Block(
                       Expression.Assign(
                           var_where
                           , Expression.Call(
                               typeof(SqlWhereClauseExtensions)
                               , nameof(SqlWhereClauseExtensions.AndName)
                               , null
                               , var_where
                               , Expression.Constant(f.ColumnName))
                           )
                       , Expression.Assign(
                           var_where
                           , Expression.Call(
                           typeof(SqlWhereClauseExtensions)
                           , nameof(SqlWhereClauseExtensions.EqValue)
                           , null
                           , var_where
                           , Expression.Convert(Expression.Property(param_m, f.FieldName), typeof(object))))
                       )
                   )
               , Expression.Call(param_deleteBuilder, nameof(SqlUpdate.Where), null, var_where)
               );

            BindDeleteValues = Expression.Lambda<Action<SqlDelete, ModelT>>(all_scripts, param_deleteBuilder, param_m).Compile();
        }

        static ModelMapper()
        {
            PrepareInsertCommand();
            PrepareMapModel();
            PrepareUpdateCommand();
            PrepareDeleteCommand();
        }

    }
}

