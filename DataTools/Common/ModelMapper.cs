using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataTools.Common
{
    /// <summary>
    /// Класс для описания логики преобразования данных в модель и обратно.
    /// </summary>
    /// <typeparam name="ModelT">Тип данных целевой модели</typeparam>
    public static class ModelMapper<ModelT> where ModelT : class, new()
    {
        private static IModelMetadata Metadata = ModelMetadata<ModelT>.Instance;

        public static Action<SqlInsert, ModelT> BindInsertValues { get; private set; }

        /// <summary>
        /// Смаппировать модель с массивом значений
        /// </summary>
        public static Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], ModelT> MapModel { get; private set; }

        /// <summary>
        /// Смаппировать модели с массивами значений
        /// </summary>
        public static Func<IDataContext, Dictionary<Type, Func<object, object>>, IEnumerable<object[]>, IEnumerable<ModelT>> MapModels { get; private set; }

        public static Action<SqlUpdate, ModelT> BindUpdateValues { get; private set; }
        public static Action<SqlDelete, ModelT> BindDeleteValues { get; private set; }

        private static void PrepareInsertCommand()
        {
            var param_insertBuilder = Expression.Parameter(typeof(SqlInsert), "insertBuilder");
            var param_m = Expression.Parameter(Metadata.ModelType, "m");

            var fields = (from field
                          in Metadata.Fields
                          where !(field.IgnoreChanges || field.Autoincrement)
                          select field).ToDictionary(p => p.FieldName);

            var call = Expression.Call(
                typeof(SqlInsertExtensions),
                nameof(SqlInsertExtensions.Value),
                typeArguments: null,
                param_insertBuilder,
                    // new object[]
                    Expression.NewArrayInit(
                        typeof(object),
                        (from field
                         in fields
                         let col = field.Value
                         select (col.IsForeignKey == false
                         ? Expression.Convert(Expression.Property(param_m, col.ColumnName), typeof(object)) as Expression
                         : Expression.Condition(
                             Expression.NotEqual(Expression.Property(param_m, col.FieldName), Expression.Convert(Expression.Constant(null), Metadata.ModelType.GetProperty(col.FieldName).PropertyType))
                             , Expression.Convert(Expression.Property(Expression.Property(param_m, col.FieldName), col.ForeignColumnName), typeof(object))
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
            var fields = Metadata.Fields;

            var param_dataContext = Expression.Parameter(typeof(IDataContext), "dataContext");
            var param_customConverters = Expression.Parameter(typeof(Dictionary<Type, Func<object, object>>), "customConverters");
            var param_dataRow = Expression.Parameter(typeof(object[]), "dataRow");

            var var_m = Expression.Variable(Metadata.ModelType, "m");
            var var_selectBuilder = Expression.Variable(typeof(SqlSelect));

            var var_customConverter = Expression.Variable(typeof(Func<object, object>), "customConverter");

            var modelAssigns = Expression.Block(
                from f
                in fields
                let unboxedType = Nullable.GetUnderlyingType(f.FieldType)
                let foreignFieldType = f.ForeignModel?.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType
                let isNullable = unboxedType != null
                let realType = isNullable ? unboxedType : f.FieldType
                let isConvertible = realType.GetInterface(nameof(IConvertible))
                let isParsable = realType.GetMethod("Parse", new Type[] { typeof(string) })
                let toString = typeof(object).GetMethod(nameof(ToString), new Type[] { })
                let fIndex = f.FieldOrder
                let arrayValue = Expression.ArrayIndex(param_dataRow, Expression.Constant(fIndex))
                select f.IsForeignKey == false
                // свойства, имеющие примитивные типы
                ? Expression.Block(
                    Expression.IfThen(
                        Expression.NotEqual(arrayValue, Expression.Constant(null))
                        , Expression.IfThenElse(
                            Expression.AndAlso(
                                Expression.NotEqual(param_customConverters, Expression.Constant(null))
                                , Expression.Call(param_customConverters, nameof(Dictionary<Type, Func<object, object>>.TryGetValue), null, Expression.Constant(realType), var_customConverter)
                                )
                            , Expression.Assign(
                                Expression.Property(var_m, f.FieldName),
                                Expression.Convert(Expression.Invoke(var_customConverter, arrayValue), f.FieldType)
                            )
                            , Expression.Assign(
                                Expression.Property(var_m, f.FieldName),
                                Expression.Convert(
                                    isConvertible != null
                                    ? Expression.Call(typeof(Convert), "ChangeType", null, arrayValue, Expression.Constant(realType))
                                    : isParsable != null
                                    ? Expression.Call(isParsable, Expression.Call(arrayValue, toString))
                                    : arrayValue as Expression
                                    , f.FieldType
                                    )
                                )
                            )
                        )
                    )
                // свойства, которые ссылаются на другие модели (внешние ключи)
                : Expression.Block(
                       variables: null,
                       Expression.IfThen(
                           Expression.NotEqual(Expression.ArrayIndex(param_dataRow, Expression.Constant(fIndex)), Expression.Constant(null)),
                           Expression.Block(
                                   Expression.Block(
                                       Expression.Assign(var_selectBuilder, Expression.New(typeof(SqlSelect)))
                                       , Expression.Call(typeof(SqlSelectExtensions), nameof(SqlSelectExtensions.From), new Type[] { f.FieldType }, var_selectBuilder)
                                       , Expression.Call(typeof(SqlSelectExtensions), nameof(SqlSelectExtensions.Where), null, var_selectBuilder, Expression.Constant(f.ForeignColumnName), arrayValue)
                                   , Expression.Assign(
                                       Expression.Property(var_m, f.FieldName)
                                       , Expression.Call(typeof(Enumerable), "FirstOrDefault", new Type[] { f.FieldType }, Expression.Call(param_dataContext, nameof(IDataContext.Select), new Type[] { f.FieldType }, var_selectBuilder, Expression.Constant(new SqlParameter[] { }, typeof(SqlParameter[])))))
                                   )
                               )
                           )
                       )
                );

            var all_variables = new ParameterExpression[] { } as IEnumerable<ParameterExpression>;
            all_variables = all_variables.Concat(new[] { var_selectBuilder, var_m, var_customConverter });

            var all_script = Expression.Block(
                    variables: all_variables
                    , Expression.Assign(var_m, Expression.New(typeof(ModelT)))
                    , modelAssigns
                    , var_m // return model;
                    );

            MapModel = Expression.Lambda<Func<IDataContext, Dictionary<Type, Func<object, object>>, object[], ModelT>>(
                all_script,
                param_dataContext,
                param_customConverters,
                param_dataRow
                ).Compile();
        }

        /// <summary>
        /// Подготовка функции для маппинга моделей за один цикл.
        /// Преимущество перед одиночным маппингом: кеширование переменных и сущностей
        /// </summary>
        private static void PrepareMapModels()
        {
            var fields = Metadata.Fields;

            var param_dataContext = Expression.Parameter(typeof(IDataContext), "dataContext");
            var param_customConverters = Expression.Parameter(typeof(Dictionary<Type, Func<object, object>>), "customConverters");
            var param_dataRows = Expression.Parameter(typeof(IEnumerable<object[]>), "dataRows");

            var var_modelsList = Expression.Variable(typeof(List<ModelT>), "modelsList");

            var var_dataRowsEnumerator = Expression.Variable(typeof(IEnumerator), "enumerator");
            var var_m = Expression.Variable(Metadata.ModelType, "m");
            var var_dataRow = Expression.Variable(typeof(object[]), "dataRow");
            var var_value = Expression.Variable(typeof(object), "value");
            var var_customConverter = Expression.Variable(typeof(Func<object, object>), "customConverter");
            LabelTarget loopBreakLabel = Expression.Label("loopBreak");

            // создать кеширующие временные словари для связанных моделей
            var modelDics = new Dictionary<string, ParameterExpression>();
            var modelVars = new Dictionary<string, ParameterExpression>();
            var modelKeys = new Dictionary<string, ParameterExpression>();
            var modelSelects = new Dictionary<string, ParameterExpression>();
            foreach (var f in fields)
            {
                if (f.IsForeignKey == false) continue;
                var foreignFieldType = f.ForeignModel.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType;
                var var_modelDict = Expression.Variable(typeof(Dictionary<,>).MakeGenericType(foreignFieldType, f.ForeignModel.ModelType), $"dict{f.FieldName}");
                modelDics[f.FieldName] = var_modelDict;
                var var_model = Expression.Variable(f.ForeignModel.ModelType, $"model{f.FieldName}");
                modelVars[f.FieldName] = var_model;
                var var_key = Expression.Variable(f.ForeignModel.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType, $"modelKey{f.FieldName}");
                modelKeys[f.FieldName] = var_key;
                var var_select = Expression.Variable(typeof(SqlSelect), $"select{f.FieldName}");
                modelSelects[f.FieldName] = var_select;
            }
            var assign_modelDics =
                from f
                in fields
                where f.IsForeignKey
                let foreignFieldType = f.ForeignModel.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType
                select
                    Expression.Assign(modelDics[f.FieldName], Expression.New(typeof(Dictionary<,>).MakeGenericType(foreignFieldType, f.ForeignModel.ModelType))) as Expression;
            var assign_modelSelects =
                from f
                in fields
                where f.IsForeignKey
                let foreignFieldType = f.ForeignModel.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType
                select Expression.Block(
                    Expression.Assign(modelSelects[f.FieldName], Expression.New(typeof(SqlSelect)))
                    , Expression.Call(typeof(SqlSelectExtensions), nameof(SqlSelectExtensions.From), new Type[] { f.FieldType }, modelSelects[f.FieldName])
                    ) as Expression;

            var all_variables = new ParameterExpression[] { } as IEnumerable<ParameterExpression>;
            all_variables = all_variables.Concat(modelDics.Select(kv => kv.Value));
            all_variables = all_variables.Concat(modelVars.Select(kv => kv.Value));
            all_variables = all_variables.Concat(modelKeys.Select(kv => kv.Value));
            all_variables = all_variables.Concat(modelSelects.Select(kv => kv.Value));
            all_variables = all_variables.Concat(new[] { var_dataRowsEnumerator, var_m, var_dataRow, var_modelsList, var_value, var_customConverter });

            var loop_dataRows = Expression.Loop(
               Expression.IfThenElse(
                   Expression.Equal(Expression.Call(var_dataRowsEnumerator, nameof(IEnumerator.MoveNext), null, null), Expression.Constant(true)),
                   Expression.Block(
                       Expression.Empty()
                       , Expression.Assign(var_m, Expression.New(typeof(ModelT)))
                       , Expression.Assign(var_dataRow, Expression.Convert(Expression.Property(var_dataRowsEnumerator, nameof(IEnumerator<object[]>.Current)), typeof(object[])))
                       , Expression.Block(
                           from f
                           in fields
                               // для поля с примитивным типом
                           let unboxedType = Nullable.GetUnderlyingType(f.FieldType)
                           let isNullable = unboxedType != null
                           let realType = isNullable ? unboxedType : f.FieldType
                           let isConvertible = realType.GetInterface(nameof(IConvertible))
                           let isParsable = realType.GetMethod("Parse", new Type[] { typeof(string) })
                           let toString = typeof(object).GetMethod("ToString", new Type[] { })

                           // для поля - внешней модели
                           let var_modelDic = f.IsForeignKey ? modelDics[f.FieldName] : null
                           let var_model = f.IsForeignKey ? modelVars[f.FieldName] : null
                           let var_modelSelect = f.IsForeignKey ? modelSelects[f.FieldName] : null
                           let var_castValue = f.IsForeignKey ? modelKeys[f.FieldName] : null

                           let foreignFieldType = f.ForeignModel?.Fields.Where(ff => ff.ColumnName == f.ForeignColumnName).First().FieldType
                           let unboxedforeignFieldType = foreignFieldType != null ? Nullable.GetUnderlyingType(foreignFieldType) : null
                           let isNullableForeign = unboxedforeignFieldType != null
                           let realForeignType = isNullableForeign ? unboxedforeignFieldType : foreignFieldType
                           let isConvertibleForeign = realForeignType?.GetInterface(nameof(IConvertible))
                           let isParsableForeign = realForeignType?.GetMethod("Parse", new Type[] { typeof(string) })

                           select Expression.Block(
                               Expression.Assign(var_value, Expression.ArrayIndex(var_dataRow, Expression.Constant(f.FieldOrder)))
                               , f.IsForeignKey == false
                               // свойства, имеющие примитивные типы
                               ? Expression.IfThen(
                                    Expression.NotEqual(var_value, Expression.Constant(null))
                                    , Expression.IfThenElse(
                                        Expression.AndAlso(
                                            Expression.NotEqual(param_customConverters, Expression.Constant(null))
                                            , Expression.Call(param_customConverters, nameof(Dictionary<Type, Func<object, object>>.TryGetValue), null, Expression.Constant(realType), var_customConverter)
                                            )
                                        , Expression.Assign(
                                            Expression.Property(var_m, f.FieldName),
                                            Expression.Convert(Expression.Invoke(var_customConverter, var_value), f.FieldType)
                                        )
                                        , Expression.Assign(
                                            Expression.Property(var_m, f.FieldName),
                                            Expression.Convert(
                                                isConvertible != null
                                                ? Expression.Call(typeof(Convert), "ChangeType", null, var_value, Expression.Constant(realType))
                                                : isParsable != null
                                                ? Expression.Call(isParsable, Expression.Call(var_value, toString))
                                                : var_value as Expression
                                                , f.FieldType
                                                )
                                            )
                                        )
                                    )
                               // собрать команды для запроса значений свойств, имеющих типы моделей
                               : Expression.IfThen(
                                   Expression.NotEqual(var_value, Expression.Constant(null)),
                                   Expression.Block(
                                       Expression.Assign(
                                           var_castValue,
                                           Expression.Convert(
                                               isConvertibleForeign != null
                                               ? Expression.Call(typeof(Convert), "ChangeType", null, var_value, Expression.Constant(realForeignType))
                                               : isParsableForeign != null
                                               ? Expression.Call(isParsableForeign, Expression.Call(var_value, toString))
                                               : var_value as Expression
                                               , foreignFieldType
                                               )
                                           )
                                       , Expression.IfThenElse(
                                           Expression.Call(var_modelDic, "TryGetValue", null, Expression.Convert(var_castValue, foreignFieldType), var_model),
                                           Expression.Assign(Expression.Property(var_m, f.FieldName), var_model)
                                           , Expression.Block(
                                            Expression.Call(typeof(SqlSelectExtensions), nameof(SqlSelectExtensions.Where), null, var_modelSelect, Expression.Constant(f.ForeignColumnName), var_value)
                                        , Expression.Assign(
                                            Expression.Property(var_m, f.FieldName)
                                            , Expression.Call(typeof(Enumerable), "FirstOrDefault", new Type[] { f.FieldType }, Expression.Call(param_dataContext, nameof(IDataContext.Select), new Type[] { f.FieldType }, var_modelSelect, Expression.Constant(new SqlParameter[] { }, typeof(SqlParameter[]))))
                                            )
                                        , Expression.Assign(Expression.Property(var_modelDic, "Item", var_castValue), Expression.Property(var_m, f.FieldName)))
                                        )
                                       )
                                   )
                               )
                           )
                       , Expression.Call(var_modelsList, nameof(List<ModelT>.Add), null, var_m)
                       )
                   , Expression.Break(loopBreakLabel)
                   )
               , loopBreakLabel);

            var all_script = Expression.Block(
                all_variables,
                Expression.Block(assign_modelDics.Concat(assign_modelSelects))
                , Expression.Assign(var_modelsList, Expression.New(typeof(List<ModelT>)))
                , Expression.Assign(var_dataRowsEnumerator, Expression.Call(param_dataRows, nameof(IEnumerable<object[]>.GetEnumerator), typeArguments: null, arguments: null))
                , loop_dataRows
                , Expression.Convert(var_modelsList, typeof(IEnumerable<ModelT>))
                );

            MapModels = Expression.Lambda<Func<IDataContext, Dictionary<Type, Func<object, object>>, IEnumerable<object[]>, IEnumerable<ModelT>>>(
                all_script,
                param_dataContext,
                param_customConverters,
                param_dataRows
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
                         where !(field.IgnoreChanges || field.Autoincrement)
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
            PrepareMapModels();
            PrepareUpdateCommand();
            PrepareDeleteCommand();
        }

    }
}

