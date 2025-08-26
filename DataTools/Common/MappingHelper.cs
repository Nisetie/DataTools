using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DataTools.Common
{
    public static class MappingHelper
    {
        private static bool ForeignKeysAreEmpty(params object[] keys)
        {
            for (int i = 0; i < keys.Length; i++)
                if (keys[i] == null)
                    return true;
            return false;
        }
        private static bool SqlParametersAreEmpty(SqlParameter[] parameters)
        {
            for (int i = 0; i < parameters.Length; i++)
                if (parameters[i].Value == null)
                    return true;
            return false;
        }

        public static string GetModelKey(params object[] keys)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                sb.Append(key == null ? null : key is byte[] bytea ? $"\"{BitConverter.ToString(bytea)}\"" : $"\"{key}\"").Append(";");
            }
            return sb.ToString();
        }

        public static IEnumerable<IModelFieldMetadata> GetPrimaryKeys(IModelMetadata modelMetadata)
        {
            List<IModelFieldMetadata> primaryKeys = new List<IModelFieldMetadata>();
            primaryKeys.AddRange(modelMetadata.GetFilterableFields());
            if (primaryKeys.Count == 0)
                foreach (var f in modelMetadata.Fields)
                    primaryKeys.Add(f);
            return primaryKeys;
        }

        public static (SqlSelect query, Dictionary<string, SqlParameter> parameters) PrepareSqlQuery(IModelMetadata metadata)
        {
            var cachedParameters = new Dictionary<string, SqlParameter>();
            var cachedWhereClause = new SqlWhere();
            var cachedSelect = new SqlSelect().From(metadata);

            var primaryKeys = GetPrimaryKeys(metadata);

            int pkCount = 0;
            foreach (var primaryKeyField in primaryKeys)
            {
                var fieldType = Type.GetType(primaryKeyField.FieldTypeName);

                if (primaryKeyField.IsForeignKey)
                {
                    int i = 0;
                    foreach (var foreignColumnName in primaryKeyField.ForeignColumnNames)
                    {
                        cachedParameters[primaryKeyField.ColumnNames[i]] = new SqlParameter(primaryKeyField.ColumnNames[i]);
                        if (pkCount == 0)
                            cachedWhereClause.Name(primaryKeyField.ColumnNames[i]).EqPar(cachedParameters[primaryKeyField.ColumnNames[i]]);
                        else
                            cachedWhereClause.AndName(primaryKeyField.ColumnNames[i]).EqPar(cachedParameters[primaryKeyField.ColumnNames[i]]);
                        i++;
                        pkCount++;
                    }
                }
                else
                {
                    cachedParameters[primaryKeyField.ColumnName] = new SqlParameter(primaryKeyField.ColumnName);
                    if (pkCount == 0)
                        cachedWhereClause.Name(primaryKeyField.ColumnName).EqPar(cachedParameters[primaryKeyField.ColumnName]);
                    else
                        cachedWhereClause.AndName(primaryKeyField.ColumnName).EqPar(cachedParameters[primaryKeyField.ColumnName]);
                    pkCount++;
                }
            }
            cachedSelect.Where(cachedWhereClause);

            return (cachedSelect, cachedParameters);

        }

        public static T PrepareGetModelKeyValue<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            var ModelName = metadata.FullObjectName;

            var primaryKeys = GetPrimaryKeys(metadata);

            // получить конкатенацию строковых представлений составного ключа модели
            var param_model = GetModelInputParameterExpressionFunction();

            var values = new List<Expression>();
            foreach (var primaryKeyField in primaryKeys)
            {
                if (primaryKeyField.IsForeignKey)
                    foreach (var foreignColumnName in primaryKeyField.ForeignColumnNames)
                        values.Add(
                            Expression.Condition(
                                Expression.NotEqual(GetModelPropertyExpressionFunction(param_model, primaryKeyField.FieldName), Expression.Constant(null))
                                , Expression.Convert(GetModelPropertyExpressionFunction(GetModelPropertyExpressionFunction(param_model, primaryKeyField.FieldName), foreignColumnName), typeof(object))
                                , Expression.Constant(null)
                                )
                            );
                else
                    values.Add(Expression.Convert(GetModelPropertyExpressionFunction(param_model, primaryKeyField.FieldName), typeof(object)));
            }
            return Expression.Lambda<T>(Expression.Call(null, typeof(MappingHelper).GetMethod(nameof(MappingHelper.GetModelKey)), Expression.NewArrayInit(typeof(object), values)), param_model).Compile();
        }

        public static T PrepareInsertCommand<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            ParameterExpression param_insertBuilder = Expression.Parameter(typeof(SqlInsert), "insertBuilder");
            ParameterExpression param_m = GetModelInputParameterExpressionFunction();

            var changeableFields = metadata.GetChangeableFields();

            var valuesFromFields = new List<Expression>();
            var valuesFromModelFields = new List<Expression>();

            foreach (var f in changeableFields)
                if (f.IsForeignKey)
                    foreach (var fk in f.ForeignColumnNames)
                        valuesFromFields.Add(Expression.Condition(
                             Expression.NotEqual(GetModelPropertyExpressionFunction(param_m, f.FieldName), Expression.Constant(null))
                             , Expression.Convert(GetModelPropertyExpressionFunction(GetModelPropertyExpressionFunction(param_m, f.FieldName), fk), typeof(object))
                             , Expression.Convert(Expression.Constant(null), typeof(object))));
                else
                    valuesFromFields.Add(Expression.Convert(GetModelPropertyExpressionFunction(param_m, f.ColumnName), typeof(object)));

            var call = Expression.Call(
                typeof(SqlInsertExtensions),
                nameof(SqlInsertExtensions.Value),
                typeArguments: null,
                param_insertBuilder, Expression.NewArrayInit(typeof(object), valuesFromFields)
                );

            var lambda = Expression.Lambda<T>(call, param_insertBuilder, param_m);
            return lambda.Compile();
        }

        /// <summary>
        /// Подготовка команды для обновления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда обновление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// </summary>
        public static T PrepareBindUpdateValuesCommand<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            var param_updateBuilder = Expression.Parameter(typeof(SqlUpdate), "updateBuilder");
            var param_m = GetModelInputParameterExpressionFunction();

            var valueExpressions = new List<Expression>();

            foreach (var field in metadata.GetChangeableFields())
            {
                if (field.IsForeignKey)
                {
                    foreach (var foreignColumn in field.ForeignColumnNames)
                        valueExpressions.Add(
                            Expression.Condition(
                                Expression.NotEqual(GetModelPropertyExpressionFunction(param_m, field.FieldName), Expression.Constant(null)),
                                Expression.Convert(GetModelPropertyExpressionFunction(GetModelPropertyExpressionFunction(param_m, field.FieldName), field.ForeignModel.GetColumn(foreignColumn).FieldName), typeof(object)),
                                Expression.Convert(Expression.Constant(null), typeof(object))
                                )
                            );
                }
                else
                    valueExpressions.Add(Expression.Convert(GetModelPropertyExpressionFunction(param_m, field.FieldName), typeof(object)));
            }

            var all_scripts = Expression.Block(
                Expression.Call(typeof(SqlUpdateExtensions), nameof(SqlUpdateExtensions.Value), null, param_updateBuilder, Expression.NewArrayInit(typeof(object), valueExpressions))
                );

            return Expression.Lambda<T>(all_scripts, param_updateBuilder, param_m).Compile();
        }

        public static T PrepareBindUpdateWhereCommand<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            var param_updateBuilder = Expression.Parameter(typeof(SqlUpdate), "updateBuilder");
            var param_m = GetModelInputParameterExpressionFunction();

            var var_where = Expression.Variable(typeof(SqlWhere));

            var whereExpressions = new List<Expression>();

            foreach (var f in metadata.GetFilterableFields())
            {
                if (f.IsForeignKey)
                {
                    for (int i = 0; i < f.ForeignColumnNames.Length; i++)
                    {
                        string foreignColumn = f.ForeignColumnNames[i];
                        string column = f.ColumnNames[i];
                        whereExpressions.Add(
                            Expression.Block(
                                Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.AndName), null, var_where, Expression.Constant(column)))
                                , Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Convert(GetModelPropertyExpressionFunction(GetModelPropertyExpressionFunction(param_m, f.FieldName), f.ForeignModel.GetColumn(foreignColumn).FieldName), typeof(object))))
                                )
                            );
                    }
                }
                else whereExpressions.Add(
                    Expression.Block(
                       Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.AndName), null, var_where, Expression.Constant(f.ColumnName)))
                       , Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Convert(GetModelPropertyExpressionFunction(param_m, f.FieldName), typeof(object))))
                       )
                    );
            }

            var all_scripts = Expression.Block(
                variables: new ParameterExpression[] { var_where }
                , Expression.Assign(var_where, Expression.New(typeof(SqlWhere)))
                , Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.Value), null, var_where, Expression.Constant(1, typeof(object)))
                , Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Constant(1, typeof(object)))
                , Expression.Block(whereExpressions)
                , Expression.Call(param_updateBuilder, nameof(SqlUpdate.Where), null, var_where)
                );

            return Expression.Lambda<T>(all_scripts, param_updateBuilder, param_m).Compile();
        }

        /// <summary>
        /// Подготовка команды для удаления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда удаление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// </summary>
        public static T PrepareDeleteWhereCommand<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            var param_deleteBuilder = Expression.Parameter(typeof(SqlDelete), "deleteBuilder");
            var param_m = GetModelInputParameterExpressionFunction();

            var var_where = Expression.Variable(typeof(SqlWhere));

            var whereExpressions = new List<Expression>();

            foreach (var f in metadata.Fields)
            {
                if (!(f.IsUnique || f.IsPrimaryKey || f.IsAutoincrement)) continue;

                if (f.IsForeignKey)
                {
                    for (int i = 0; i < f.ForeignColumnNames.Length; i++)
                    {
                        string foreignColumn = f.ForeignColumnNames[i];
                        string column = f.ColumnNames[i];
                        whereExpressions.Add(
                            Expression.Block(
                                Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.AndName), null, var_where, Expression.Constant(column)))
                                , Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Convert(GetModelPropertyExpressionFunction(GetModelPropertyExpressionFunction(param_m, f.FieldName), f.ForeignModel.GetColumn(foreignColumn).FieldName), typeof(object))))
                                )
                            );
                    }
                }
                else whereExpressions.Add(
                    Expression.Block(
                       Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.AndName), null, var_where, Expression.Constant(f.ColumnName)))
                       , Expression.Assign(var_where, Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Convert(GetModelPropertyExpressionFunction(param_m, f.FieldName), typeof(object)))))
                    );
            }

            var all_scripts = Expression.Block(
               variables: new ParameterExpression[] { var_where }
               , Expression.Assign(var_where, Expression.New(typeof(SqlWhere)))
               , Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.Value), null, var_where, Expression.Constant(1, typeof(object)))
               , Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Constant(1, typeof(object)))
               , Expression.Block(whereExpressions)
               , Expression.Call(param_deleteBuilder, nameof(SqlUpdate.Where), null, var_where)
               );

            return Expression.Lambda<T>(all_scripts, param_deleteBuilder, param_m).Compile();
        }

        /// <summary>
        /// Подготовка функции для маппинга только одной модели
        /// </summary>
        public static T PrepareMapModel<T>(
            IModelMetadata metadata,
            Func<ParameterExpression> GetModelVariableExpressionFunction,
            Func<ParameterExpression, string, Expression> GetModelPropertyExpressionFunction,
            Func<ParameterExpression, string, Expression, Expression> GetModelPropertySetterExpressionFunction,
            Func<IModelMetadata, ParameterExpression> GetForeignModelCacheVariableExpressionFunction,
            Func<IModelFieldMetadata, ParameterExpression> GetForeignModelVariableExpressionFunction,
            Func<ParameterExpression> GetTargetModelCacheVariableExpressionFunction,
            Func<ParameterExpression, IModelMetadata, Expression> GetCallGetModelCacheExpressionFunction,
            Func<IModelMetadata, ParameterExpression, ParameterExpression, ParameterExpression, ParameterExpression, Expression> GetInvokeMapModelExpressionFunction,
            Func<ParameterExpression, Expression> GetLocalModelAssignNewExpressionFunction
            )
        {
            ParameterExpression param_dataContext = Expression.Parameter(typeof(IDataContext), "dataContext");
            ParameterExpression param_customTypeConverters = Expression.Parameter(typeof(Dictionary<Type, Func<object, object>>), "customConverters");
            ParameterExpression param_dataRow = Expression.Parameter(typeof(object[]), "dataRow");
            ParameterExpression var_m = GetModelVariableExpressionFunction();
            ParameterExpression var_value = Expression.Variable(typeof(object), "value");
            ParameterExpression var_customConverter = Expression.Variable(typeof(Func<object, object>), "customConverter");
            ParameterExpression param_queryCache = Expression.Parameter(typeof(SelectCache), "queryCache");
            ParameterExpression var_foreignModelQueryResult = Expression.Variable(typeof(object[]), "foreignModelQueryResult");

            var fields = from f in metadata.Fields orderby f.FieldOrder select f;

            bool containsForeignKey = fields.Any(f => f.IsForeignKey);

            // создать кеширующие временные словари для связанных моделей
            var foreignModelCaches = new Dictionary<string, ParameterExpression>();
            var foreignModelVariables = new Dictionary<string, ParameterExpression>();
            var foreignModelSqlParameters = new Dictionary<string, Dictionary<string, ParameterExpression>>();
            var foreignModelKeys = new Dictionary<string, Dictionary<string, Expression>>();
            var foreignModelKeysArray = new Dictionary<string, ParameterExpression>();
            var foreignModelSelects = new Dictionary<string, ParameterExpression>();
            foreach (var f in fields)
            {
                if (f.IsForeignKey == false) continue;
                foreignModelCaches[f.FieldName] = GetForeignModelCacheVariableExpressionFunction(f.ForeignModel);
                foreignModelVariables[f.FieldName] = GetForeignModelVariableExpressionFunction(f);


                var certaintForeignModelKeys = new Dictionary<string, Expression>();
                var certainForeignModelSqlParameters = new Dictionary<string, ParameterExpression>();
                foreach (var fk in f.ForeignColumnNames)
                {
                    certaintForeignModelKeys[fk] = Expression.Variable(typeof(object), $"modelKey{f.FieldName}_{fk}");
                    certainForeignModelSqlParameters[fk] = Expression.Variable(typeof(SqlParameter), $"model_{f.FieldName}_sqlParameter");
                }
                foreignModelKeys[f.FieldName] = certaintForeignModelKeys;
                foreignModelKeysArray[f.FieldName] = Expression.Variable(typeof(object[]), $"modelKeyArray_{f.FieldName}");
                foreignModelSqlParameters[f.FieldName] = certainForeignModelSqlParameters;
                foreignModelSelects[f.FieldName] = Expression.Variable(typeof(SqlSelect), $"select{f.FieldName}");
            }
            var var_modelCache = GetTargetModelCacheVariableExpressionFunction();

            var all_variables = new ParameterExpression[] { } as IEnumerable<ParameterExpression>;
            all_variables = all_variables.Concat(foreignModelCaches.Select(kv => kv.Value));
            all_variables = all_variables.Concat(foreignModelVariables.Select(kv => kv.Value));

            foreach (var fmk in foreignModelKeys)
                all_variables = all_variables.Concat(fmk.Value.Select(kv => kv.Value as ParameterExpression));

            all_variables = all_variables.Concat(foreignModelSelects.Select(kv => kv.Value));

            all_variables = all_variables.Concat(foreignModelKeysArray.Select(kv => kv.Value));

            foreach (var fmp in foreignModelSqlParameters)
                all_variables = all_variables.Concat(fmp.Value.Select(kv => kv.Value));

            all_variables = all_variables.Concat(new[] { var_m, var_value, var_customConverter, var_foreignModelQueryResult, var_modelCache });


            var blockSimpleProperties = Expression.Block(
                variables: null,
                Expression.Block(
                    from f
                    in fields
                    where !f.IsForeignKey
                    // для поля с простым типом данных
                    let isPrimaryKey = f.IsPrimaryKey || f.IsAutoincrement
                    let fieldType = Type.GetType(f.FieldTypeName)
                    let UnboxedType = Nullable.GetUnderlyingType(fieldType) // TODO GetFieldType (ModelT or DynamicModel)
                    let IsNullable = UnboxedType != null
                    let RealType = IsNullable ? UnboxedType : fieldType
                    let IsConvertible = RealType.GetInterface(nameof(IConvertible)) != null
                    let ParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string) })
                    let IsParsable = ParseMethod != null
                    let ToStringMethod = typeof(object).GetMethod(nameof(ToString), new Type[] { })                    
                    orderby f.FieldOrder
                    select Expression.Block(
                        Expression.Assign(var_value, Expression.ArrayIndex(param_dataRow, Expression.Constant(f.FieldOrder)))
                        // свойства, имеющие примитивные типы
                        , Expression.IfThenElse(
                            Expression.Equal(var_value, Expression.Constant(null))
                            , GetModelPropertySetterExpressionFunction(var_m, f.FieldName, Expression.Default(fieldType))
                            , Expression.Block(
                                Expression.IfThenElse(
                                    Expression.Call(param_customTypeConverters, nameof(Dictionary<Type, Func<object, object>>.TryGetValue), null, Expression.Constant(RealType), var_customConverter)
                                    , GetModelPropertySetterExpressionFunction(var_m, f.FieldName, Expression.Convert(Expression.Invoke(var_customConverter, var_value), fieldType))
                                    , GetModelPropertySetterExpressionFunction(var_m, f.FieldName,
                                    IsConvertible
                                    ? Expression.Convert(Expression.Call(typeof(Convert), "ChangeType", null, var_value, Expression.Constant(RealType)), fieldType)
                                    : IsParsable
                                    ? Expression.Convert(Expression.Call(ParseMethod, Expression.Call(var_value, "ToString", null, null)), fieldType)
                                    : Expression.Convert(var_value, fieldType)
                                    )
                                )
                                )
                            )
                        )
                    )
                , Expression.Assign(var_modelCache, GetCallGetModelCacheExpressionFunction(param_queryCache, metadata))
                , Expression.Call(var_modelCache, nameof(SelectDynamicCache.AddModel), null, var_m)
                );

            var foreignKeysDenormalized = new List<(IModelFieldMetadata field, IModelMetadata foreignModel, string foreignColumn, int columnOrder)>();

            foreach (var f in (from f in fields where f.IsForeignKey orderby f.FieldOrder select f))
            {
                int columnOrder = 0;
                foreach (var foreignColumnName in f.ForeignColumnNames)
                    foreignKeysDenormalized.Add((f, f.ForeignModel, foreignColumnName, f.FieldOrder + columnOrder++));
            }

            var block_prep = Expression.Block(
                    from f in fields
                    where f.IsForeignKey
                    orderby f.FieldOrder
                    select Expression.Assign(foreignModelCaches[f.FieldName], GetCallGetModelCacheExpressionFunction(param_queryCache, f.ForeignModel))
                    );

            var blockForeignModels = Expression.Block(
                // перебор всех полей внешних ключей и присвоение их значений из внешних моделей
                Expression.Block(
                    from f in foreignKeysDenormalized
                    let fieldName = f.field.FieldName
                    // для поля с простым типом данных
                    let fieldType = Type.GetType(f.foreignModel.GetColumn(f.foreignColumn).FieldTypeName)
                    let UnboxedType = Nullable.GetUnderlyingType(fieldType)
                    let IsNullable = UnboxedType != null
                    let RealType = IsNullable ? UnboxedType : fieldType
                    let IsConvertible = RealType.GetInterface(nameof(IConvertible)) != null
                    let ParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string) })
                    let IsParsable = ParseMethod != null
                    let ToStringMethod = typeof(object).GetMethod(nameof(ToString), new Type[] { })
                    // для поля - внешней модели
                    let var_foreignModelCache = foreignModelCaches[fieldName]
                    let var_foreignKeyValue = foreignModelKeys[fieldName][f.foreignModel.GetColumn(f.foreignColumn).FieldName]
                    let var_foreignKeySqlParameter = foreignModelSqlParameters[fieldName][f.foreignModel.GetColumn(f.foreignColumn).FieldName]
                    select Expression.Block(
                        Expression.Assign(Expression.Property(Expression.Property(Expression.Field(var_foreignModelCache, nameof(SelectDynamicCache.Parameters)), "Item", Expression.Constant(f.foreignColumn)), nameof(SqlParameter.Value)), Expression.Assign(var_foreignKeyValue, Expression.ArrayIndex(param_dataRow, Expression.Constant(f.columnOrder))))
                    )
                    )
                // поиск внешних моделей в кеше или их запрос из БД
                , Expression.Block(
                    from f in fields
                    where f.IsForeignKey
                    orderby f.FieldOrder
                    let fieldName = f.FieldName
                    // для поля - внешней модели
                    let var_foreignModelCache = foreignModelCaches[fieldName]
                    let var_foreignModel = foreignModelVariables[fieldName]
                    let var_foreignModelSelect = foreignModelSelects[fieldName]
                    let var_foreignKeysArray = foreignModelKeysArray[fieldName]
                    select Expression.Block(
                        variables: null
                        , Expression.Assign(var_foreignKeysArray, Expression.NewArrayInit(typeof(object), foreignModelKeys[fieldName].Select(kv => kv.Value)))
                        , Expression.IfThen(
                            Expression.Equal(Expression.Call(typeof(MappingHelper), nameof(MappingHelper.ForeignKeysAreEmpty), null, var_foreignKeysArray), Expression.Constant(false))
                            , Expression.IfThenElse(
                                Expression.Call(
                                    var_foreignModelCache
                                    , nameof(SelectDynamicCache.TryGetModelByKeys)
                                    , null
                                    , var_foreignModel, var_foreignKeysArray
                                    )
                                , GetModelPropertySetterExpressionFunction(var_m, f.FieldName, var_foreignModel)
                                , Expression.Block(
                                    variables: null
                                    , Expression.Assign(var_foreignModelSelect, Expression.Field(GetCallGetModelCacheExpressionFunction(param_queryCache, f.ForeignModel), nameof(SelectDynamicCache.CachedSelect)))
                                    , Expression.Assign(
                                        var_foreignModelQueryResult
                                        , Expression.Convert(
                                            Expression.Call(
                                                typeof(Enumerable)
                                                , "FirstOrDefault", new Type[] { typeof(object[]) }
                                                , Expression.Call(
                                                    param_dataContext
                                                    , nameof(IDataContext.ExecuteWithResult)
                                                    , null
                                                    , var_foreignModelSelect
                                                    , Expression.Field(var_foreignModelCache, nameof(SelectDynamicCache.ParametersArray)))), typeof(object[]))
                                        )
                                    , Expression.IfThen(
                                        Expression.NotEqual(var_foreignModelQueryResult, Expression.Constant(null))
                                        , Expression.Block(
                                            Expression.Assign(var_foreignModel, GetInvokeMapModelExpressionFunction(f.ForeignModel, param_dataContext, param_customTypeConverters, var_foreignModelQueryResult, param_queryCache))
                                            , GetModelPropertySetterExpressionFunction(var_m, f.FieldName, var_foreignModel)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                );

            var all_script = Expression.Block(
                all_variables
                , GetLocalModelAssignNewExpressionFunction(var_m)
                , blockSimpleProperties
                , block_prep
                , blockForeignModels
                , var_m
                );

            return
                Expression.Lambda<T>(
                all_script,
                param_dataContext,
                param_customTypeConverters,
                param_dataRow,
                param_queryCache
                ).Compile();
        }
    }
}

