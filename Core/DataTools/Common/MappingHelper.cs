using DataTools.Attributes;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DataTools.Common
{
    public static class MappingHelper
    {
        public static SqlSelect GetForeignSelect(IModelMetadata from, IModelMetadata to)
        {
            var cachedParameters = new Dictionary<string, SqlParameter>();
            var cachedWhereClause = new SqlWhere();
            var sqlSelect = new SqlSelect().From(to).Select(to);

            var fields = from.Fields;

            int pkCount = 0;
            foreach (var field in fields)
            {
                if (field.IsForeignKey == false || field.ForeignModel.ModelName != to.ModelName) continue;

                var fieldType = Type.GetType(field.FieldTypeName);

                foreach (var foreignColumnName in field.ForeignColumnNames)
                {
                    var ffn = to.GetColumn(foreignColumnName);
                    cachedParameters[ffn.ColumnName] = new SqlParameter(ffn.ColumnName);
                    if (pkCount == 0)
                        cachedWhereClause.Name(ffn.ColumnName).EqPar(cachedParameters[ffn.ColumnName]);
                    else
                        cachedWhereClause.AndName(ffn.ColumnName).EqPar(cachedParameters[ffn.ColumnName]);
                    pkCount++;
                }
                sqlSelect.Where(cachedWhereClause);
            }
            return sqlSelect;
        }

        public static string GetModelUniqueString(params object[] keys)
        {
            StringBuilder sb = new StringBuilder(8);
            for (int i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
                sb.Append(key == null ? null : key is byte[] bytea ? BitConverter.ToString(bytea) : key.ToString()).Append(";");
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
            return Expression.Lambda<T>(Expression.Call(null, typeof(MappingHelper).GetMethod(nameof(MappingHelper.GetModelUniqueString)), Expression.NewArrayInit(typeof(object), values)), param_model).Compile();
        }

        /// <summary>
        /// Подготовка команды для обновления сущности на стороне источника данных.
        /// Внимание! Если сущность не имеет уникального ключа, тогда обновление недопустимо.
        /// Так как непонятно, по какому признаку фильтровать записи на стороне источника.
        /// </summary>
        public static T PrepareGetArrayOfValuesCommand<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            var param_m = GetModelInputParameterExpressionFunction();

            var valueExpressions = new List<Expression>();

            foreach (var field in metadata.GetChangeableFields())
                if (field.IsForeignKey)
                    foreach (var foreignColumn in field.ForeignColumnNames)
                        valueExpressions.Add(Expression.Condition(
                                Expression.NotEqual(GetModelPropertyExpressionFunction(param_m, field.FieldName), Expression.Constant(null))
                                , Expression.Convert(GetModelPropertyExpressionFunction(GetModelPropertyExpressionFunction(param_m, field.FieldName), field.ForeignModel.GetColumn(foreignColumn).FieldName), typeof(object)),
                                Expression.Convert(Expression.Constant(null), typeof(object))
                                )
                            );
                else
                    valueExpressions.Add(Expression.Convert(GetModelPropertyExpressionFunction(param_m, field.FieldName), typeof(object)));

            var all_scripts = Expression.Block(Expression.NewArrayInit(typeof(object), valueExpressions));

            return Expression.Lambda<T>(all_scripts, param_m).Compile();
        }

        public static T PrepareCreateWhereClause<T>(IModelMetadata metadata, Func<ParameterExpression> GetModelInputParameterExpressionFunction, Func<Expression, string, Expression> GetModelPropertyExpressionFunction)
        {
            var param_m = GetModelInputParameterExpressionFunction();

            var var_where = Expression.Variable(typeof(SqlWhere));

            var whereExpressions = new List<Expression>();

            foreach (var f in metadata.NoUniqueKey ? metadata.Fields : metadata.GetFilterableFields())
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
                // проверка метамодели на отсутствие идентификационного поля
                , metadata.NoUniqueKey == false && (!metadata.Fields.Any((f) => f.IsUnique || f.IsAutoincrement || f.IsPrimaryKey))
                ? Expression.Throw(Expression.Constant($"Анализ {metadata.ModelTypeName}... Нет уникальных полей с атрибутами {nameof(UniqueAttribute)}/{nameof(AutoincrementAttribute)}/{nameof(PrimaryKeyAttribute)}! Вы хотите изменить ВСЕ строки?! Укажите в метамодели как минимум одно поле с атрибутом {nameof(UniqueAttribute)}/{nameof(AutoincrementAttribute)}/{nameof(PrimaryKeyAttribute)}. Или используйте атрибут {nameof(NoUniqueAttribute)}, чтобы игнорировать проверку на уникальность."))
                : Expression.Empty() as Expression
                , Expression.Assign(var_where, Expression.New(typeof(SqlWhere)))
                , Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.Value), null, var_where, Expression.Constant(1, typeof(object)))
                , Expression.Call(typeof(SqlWhereExtensions), nameof(SqlWhereExtensions.EqValue), null, var_where, Expression.Constant(1, typeof(object)))
                , Expression.Block(whereExpressions)
                , var_where
                );

            return Expression.Lambda<T>(all_scripts, param_m).Compile();
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
            Func<ParameterExpression, IModelMetadata, ParameterExpression, ParameterExpression, ParameterExpression, ParameterExpression, Expression> GetInvokeMapModelExpressionFunction,
            Func<IModelMetadata, ParameterExpression, Expression> GetModelAssignNewExpressionFunction
            )
        {
            ParameterExpression param_dataContext = Expression.Parameter(typeof(IDataContext), "dataContext");
            ParameterExpression param_customTypeConverters = Expression.Parameter(typeof(Dictionary<Type, Func<object, object>>), "customConverters");
            ParameterExpression param_dataRow = Expression.Parameter(typeof(object[]), "dataRow");
            ParameterExpression param_m = GetModelVariableExpressionFunction();
            ParameterExpression var_value = Expression.Variable(typeof(object), "value");
            ParameterExpression var_customConverter = Expression.Variable(typeof(Func<object, object>), "customConverter");
            ParameterExpression param_queryCache = Expression.Parameter(typeof(SelectCache), "queryCache");
            ParameterExpression var_foreignModelQueryResult = Expression.Variable(typeof(object[]), "foreignModelQueryResult");
            ParameterExpression var_sb = Expression.Variable(typeof(StringBuilder));

            var fields = from f in metadata.Fields orderby f.FieldOrder select f;

            bool containsForeignKey = fields.Any(f => f.IsForeignKey);

            // создать кеширующие временные словари для связанных моделей
            var foreignModelCaches = new Dictionary<string, ParameterExpression>();
            var foreignModelVariables = new Dictionary<string, ParameterExpression>();
            var foreignModelSqlParameters = new Dictionary<string, Dictionary<string, ParameterExpression>>();
            var foreignModelKeys = new Dictionary<string, Dictionary<string, Expression>>();
            var foreignModelKeysEmptyChecks = new Dictionary<string, Dictionary<string, Expression>>();
            var foreignModelKeysEmptyCheck = new Dictionary<string, Expression>();
            var foreignModelKeysStringBuilders = new Dictionary<string, ParameterExpression>();
            var foreignModelKeysArray = new Dictionary<string, ParameterExpression>();
            var foreignModelSelects = new Dictionary<string, ParameterExpression>();
            var foreignModelSelectAssigns = new Dictionary<string, Expression>();
            foreach (var f in fields)
            {
                if (f.IsForeignKey == false) continue;
                foreignModelCaches[f.FieldName] = GetForeignModelCacheVariableExpressionFunction(f.ForeignModel);
                foreignModelVariables[f.FieldName] = GetForeignModelVariableExpressionFunction(f);


                var certaintForeignModelKeys = new Dictionary<string, Expression>();
                var certainForeignModelSqlParameters = new Dictionary<string, ParameterExpression>();
                foreignModelKeysEmptyChecks[f.FieldName] = new Dictionary<string, Expression>();
                foreach (var fcn in f.ForeignColumnNames)
                {
                    certaintForeignModelKeys[fcn] = Expression.Variable(typeof(object), $"modelKey{f.FieldName}_{fcn}");
                    certainForeignModelSqlParameters[fcn] = Expression.Variable(typeof(SqlParameter), $"model_{f.FieldName}_sqlParameter");
                    foreignModelKeysEmptyChecks[f.FieldName][fcn] = Expression.NotEqual(certaintForeignModelKeys[fcn], Expression.Constant(null));
                }
                foreignModelKeys[f.FieldName] = certaintForeignModelKeys;
                foreignModelKeysArray[f.FieldName] = Expression.Variable(typeof(object[]), $"modelKeyArray_{f.FieldName}");
                foreignModelSqlParameters[f.FieldName] = certainForeignModelSqlParameters;
                foreignModelSelects[f.FieldName] = Expression.Variable(typeof(SqlSelect), $"select{f.FieldName}");

                foreignModelKeysStringBuilders[f.FieldName] = Expression.Variable(typeof(StringBuilder), $"sb{f.FieldName}");
            }
            var var_modelCache = GetTargetModelCacheVariableExpressionFunction();

            var all_variables = new ParameterExpression[] { } as IEnumerable<ParameterExpression>;
            all_variables = all_variables.Concat(foreignModelCaches.Select(kv => kv.Value));
            all_variables = all_variables.Concat(foreignModelVariables.Select(kv => kv.Value));

            foreach (var fmk in foreignModelKeys)
            {
                all_variables = all_variables.Concat(fmk.Value.Select(kv => kv.Value as ParameterExpression));

                if (fmk.Value.Count == 1)
                    foreignModelKeysEmptyCheck[fmk.Key] = foreignModelKeysEmptyChecks[fmk.Key].Values.First();
                else
                {
                    var checks = foreignModelKeysEmptyChecks[fmk.Key].Values.ToArray();
                    int pairs = checks.Length >> 1;
                    if (checks.Length % 2 == 1)
                        foreignModelKeysEmptyCheck[fmk.Key] = checks[checks.Length - 1];
                    else
                        foreignModelKeysEmptyCheck[fmk.Key] = Expression.AndAlso(checks[0], checks[1]);
                    for (int i = 2; i < pairs; i += 2)
                        foreignModelKeysEmptyCheck[fmk.Key] = Expression.AndAlso(foreignModelKeysEmptyCheck[fmk.Key], Expression.AndAlso(checks[i], checks[i + 1]));
                }
            }

            all_variables = all_variables.Concat(foreignModelSelects.Select(kv => kv.Value));

            all_variables = all_variables.Concat(foreignModelKeysArray.Select(kv => kv.Value));

            all_variables = all_variables.Concat(foreignModelKeysStringBuilders.Select(kv => kv.Value));

            foreach (var fmp in foreignModelSqlParameters)
                all_variables = all_variables.Concat(fmp.Value.Select(kv => kv.Value));

            all_variables = all_variables.Concat(new[] { var_value, var_customConverter, var_foreignModelQueryResult, var_modelCache, var_sb });

            var blockSimplePropertiesWithCustomConverters = Expression.Block(
                variables: null,
                Expression.Block(
                    from f
                    in fields
                    where !f.IsForeignKey
                    // для поля с простым типом данных
                    let isPrimaryKey = f.IsPrimaryKey || f.IsAutoincrement
                    let fieldType = Type.GetType(f.FieldTypeName)
                    let UnboxedType = Nullable.GetUnderlyingType(fieldType)
                    let IsNullable = UnboxedType != null
                    let RealType = IsNullable ? UnboxedType : fieldType
                    let IsConvertible = typeof(IConvertible).IsAssignableFrom(RealType)
                    let ParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string) })
                    let DateTimeOffsetParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string), typeof(IFormatProvider), typeof(DateTimeStyles) })
                    let stringBuilderAppend = typeof(StringBuilder).GetMethod("Append", new Type[] { typeof(string) })
                    let IsParsable = ParseMethod != null
                    let IsTrueNullable = f.IsNullable                    
                    orderby f.FieldOrder
                    select Expression.Block(
                        Expression.Assign(var_value, Expression.ArrayIndex(param_dataRow, Expression.Constant(f.FieldOrder)))
                        // свойства, имеющие примитивные типы
                        , Expression.IfThenElse(
                            Expression.Equal(var_value, Expression.Constant(null))
                            , Expression.Block(
                                GetModelPropertySetterExpressionFunction(param_m, f.FieldName, Expression.Default(fieldType)),
                                isPrimaryKey
                                ? Expression.Call(var_sb, stringBuilderAppend, Expression.Constant("NULL;"))
                                : Expression.Empty() as Expression
                                )
                            , Expression.Block(
                                Expression.IfThenElse(
                                    Expression.Call(param_customTypeConverters, nameof(Dictionary<Type, Func<object, object>>.TryGetValue), null, Expression.Constant(RealType), var_customConverter)
                                    , GetModelPropertySetterExpressionFunction(param_m, f.FieldName, Expression.Convert(Expression.Invoke(var_customConverter, var_value), fieldType))
                                    , GetModelPropertySetterExpressionFunction(param_m, f.FieldName,
                                    GetValueConvertedExpression(fieldType, RealType, IsConvertible, ParseMethod, DateTimeOffsetParseMethod, IsParsable)
                                    )
                                )
                                , isPrimaryKey
                                ? Expression.Block(
                                    Expression.Call(var_sb, stringBuilderAppend, Expression.Constant("\""))
                                    , Expression.Call(var_sb, stringBuilderAppend,
                                    RealType == typeof(byte[])
                                    ? Expression.Call(null, typeof(BitConverter).GetMethod("ToString", new Type[] { typeof(byte[]) }), GetModelPropertyExpressionFunction(param_m, f.FieldName))
                                    : Expression.Call(GetModelPropertyExpressionFunction(param_m, f.FieldName), "ToString", null, null)
                                    )
                                    , Expression.Call(var_sb, stringBuilderAppend, Expression.Constant("\";"))
                                    )
                                : Expression.Empty() as Expression
                                )
                            )
                        )
                    )
                , Expression.Assign(var_modelCache, GetCallGetModelCacheExpressionFunction(param_queryCache, metadata))
                , Expression.Call(var_modelCache, nameof(SelectDynamicCache.AddModel), null, Expression.Call(var_sb, "ToString", null, null), param_m)
                );
            var blockSimplePropertiesIfCustomConvertersIsNull = Expression.Block(
                variables: null,
                Expression.Block(
                    from f
                    in fields
                    where !f.IsForeignKey
                    // для поля с простым типом данных
                    let isPrimaryKey = f.IsPrimaryKey || f.IsAutoincrement || f.IsUnique
                    let fieldType = Type.GetType(f.FieldTypeName)
                    let UnboxedType = Nullable.GetUnderlyingType(fieldType)
                    let IsNullable = UnboxedType != null
                    let RealType = IsNullable ? UnboxedType : fieldType
                    let IsConvertible = typeof(IConvertible).IsAssignableFrom(RealType)
                    let ParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string) })
                    let DateTimeOffsetParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string), typeof(IFormatProvider), typeof(DateTimeStyles) })
                    let stringBuilderAppend = typeof(StringBuilder).GetMethod("Append", new Type[] { typeof(string) })
                    let IsParsable = ParseMethod != null
                    orderby f.FieldOrder
                    select Expression.Block(
                        Expression.Assign(var_value, Expression.ArrayIndex(param_dataRow, Expression.Constant(f.FieldOrder)))
                        // свойства, имеющие примитивные типы
                        , Expression.IfThenElse(
                            Expression.Equal(var_value, Expression.Constant(null))
                            , Expression.Block(
                                GetModelPropertySetterExpressionFunction(param_m, f.FieldName, Expression.Default(fieldType)),
                                isPrimaryKey
                                ? Expression.Call(var_sb, stringBuilderAppend, Expression.Constant("NULL;"))
                                : Expression.Empty() as Expression
                                )
                            , Expression.Block(
                                GetModelPropertySetterExpressionFunction(param_m, f.FieldName,
                                GetValueConvertedExpression(fieldType, RealType, IsConvertible, ParseMethod, DateTimeOffsetParseMethod, IsParsable)
                                )
                                , isPrimaryKey
                                ? Expression.Block(
                                    Expression.Call(var_sb, stringBuilderAppend, Expression.Constant("\""))
                                    , Expression.Call(var_sb, stringBuilderAppend,
                                    RealType == typeof(byte[])
                                    ? Expression.Call(null, typeof(BitConverter).GetMethod("ToString", new Type[] { typeof(byte[]) }), GetModelPropertyExpressionFunction(param_m, f.FieldName))
                                    : Expression.Call(GetModelPropertyExpressionFunction(param_m, f.FieldName), "ToString", null, null)
                                    )
                                    , Expression.Call(var_sb, stringBuilderAppend, Expression.Constant("\";"))
                                    )
                                : Expression.Empty() as Expression
                                )
                            )
                        )
                    )
                , Expression.Assign(var_modelCache, GetCallGetModelCacheExpressionFunction(param_queryCache, metadata))
                , Expression.Call(var_modelCache, nameof(SelectDynamicCache.AddModel), null, Expression.Call(var_sb, "ToString", null, null), param_m)
                );

            var blockSimpleProperties = Expression.IfThenElse(
                Expression.IsTrue(
                    Expression.OrElse(
                        Expression.Equal(param_customTypeConverters, Expression.Constant(null)),
                        Expression.Equal(Expression.Property(param_customTypeConverters, nameof(Dictionary<Type, Func<object, object>>.Count)), Expression.Constant(0)))
                    ),
                blockSimplePropertiesIfCustomConvertersIsNull,
                blockSimplePropertiesWithCustomConverters
                );

            var foreignKeysDenormalized = new List<(IModelFieldMetadata field, IModelMetadata foreignModel, string foreignColumn, int columnOrder)>();

            foreach (var f in (from f in fields where f.IsForeignKey orderby f.FieldOrder select f))
            {
                int columnOrder = 0;
                foreach (var foreignColumnName in f.ForeignColumnNames)
                    foreignKeysDenormalized.Add((f, f.ForeignModel, foreignColumnName, f.FieldOrder + columnOrder++));
            }

            BlockExpression block_prep;
            if (foreignKeysDenormalized.Count == 0)
                block_prep = Expression.Block();
            else
                block_prep = Expression.Block(
                from f in fields
                where f.IsForeignKey
                orderby f.FieldOrder
                select Expression.Block(
                    Expression.Assign(foreignModelCaches[f.FieldName], GetCallGetModelCacheExpressionFunction(param_queryCache, f.ForeignModel)),
                    Expression.Assign(foreignModelKeysStringBuilders[f.FieldName], Expression.New(typeof(StringBuilder)))
                    )
                );

            BlockExpression blockForeignModels;
            if (foreignKeysDenormalized.Count == 0)
                blockForeignModels = Expression.Block();
            else
                blockForeignModels = Expression.Block(
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
                        let DateTimeOffsetParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string), typeof(IFormatProvider), typeof(DateTimeStyles) })
                        let IsParsable = ParseMethod != null
                        // для поля - внешней модели
                        let var_foreignKeyValue = foreignModelKeys[fieldName][f.foreignModel.GetColumn(f.foreignColumn).FieldName]
                        let var_foreignKeySqlParameter = foreignModelSqlParameters[fieldName][f.foreignModel.GetColumn(f.foreignColumn).FieldName]
                        let var_foreignStringBuilder = foreignModelKeysStringBuilders[fieldName]
                        select Expression.Block(
                            Expression.Assign(var_foreignKeyValue, Expression.ArrayIndex(param_dataRow, Expression.Constant(f.columnOrder))),
                            Expression.IfThenElse(
                                Expression.Equal(var_foreignKeyValue, Expression.Constant(null)),
                                Expression.Call(var_foreignStringBuilder, typeof(StringBuilder).GetMethod("Append", new Type[] { typeof(string) }), Expression.Constant("NULL;")),
                                Expression.Block(
                                    Expression.Assign(var_foreignKeyValue,
                                        RealType == typeof(DateTimeOffset)
                                        ? Expression.Convert(Expression.Call(DateTimeOffsetParseMethod, Expression.Call(var_foreignKeyValue, "ToString", null, null), Expression.Constant(null, typeof(IFormatProvider)), Expression.Constant(DateTimeStyles.AssumeUniversal)), typeof(object))
                                        : IsConvertible
                                        ? Expression.Convert(Expression.Call(typeof(Convert), "ChangeType", null, var_foreignKeyValue, Expression.Constant(RealType)), typeof(object))
                                        : IsParsable
                                        ? Expression.Convert(Expression.Call(ParseMethod, Expression.Call(var_foreignKeyValue, "ToString", null, null)), typeof(object))
                                        : Expression.Convert(var_foreignKeyValue, typeof(object))
                                        ),
                                    Expression.Block(
                                        Expression.Call(var_foreignStringBuilder, typeof(StringBuilder).GetMethod("Append", new Type[] { typeof(string) }), Expression.Constant("\""))
                                        , Expression.Call(var_foreignStringBuilder, typeof(StringBuilder).GetMethod("Append", new Type[] { typeof(string) }),
                                        RealType == typeof(byte[])
                                        ? Expression.Call(null, typeof(BitConverter).GetMethod("ToString", new Type[] { typeof(byte[]) }), var_foreignKeyValue)
                                        : Expression.Call(var_foreignKeyValue, "ToString", null, null)
                                        )
                                        , Expression.Call(var_foreignStringBuilder, typeof(StringBuilder).GetMethod("Append", new Type[] { typeof(string) }), Expression.Constant("\";"))
                                        )
                                    )
                                ),
                            Expression.Assign(
                                var_foreignKeySqlParameter,
                                Expression.New(
                                    typeof(SqlParameter).GetConstructor(new Type[] { typeof(string), typeof(object) }),
                                    Expression.Constant(f.foreignModel.GetColumn(f.foreignColumn).FieldName),
                                    var_foreignKeyValue
                                    )
                                )
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
                        let var_foreignStringBuilder = foreignModelKeysStringBuilders[fieldName]
                        select Expression.Block(
                            variables: null
                            , Expression.IfThenElse(
                                Expression.Call(
                                    var_foreignModelCache
                                    , nameof(SelectDynamicCache.TryGetModelByKey)
                                    , null
                                    , var_foreignModel, Expression.Call(var_foreignStringBuilder, "ToString", null, null) //, var_foreignKeysArray
                                    )
                                , GetModelPropertySetterExpressionFunction(param_m, f.FieldName, var_foreignModel)
                                , Expression.Block(
                                    variables: null
                                    , GetModelAssignNewExpressionFunction(f.ForeignModel, var_foreignModel)
                                    , Expression.Assign(var_foreignModelSelect, Expression.Constant(GetForeignSelect(metadata, f.ForeignModel)))
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
                                                    , Expression.NewArrayInit(typeof(SqlParameter), foreignModelSqlParameters[fieldName].Values.Select(v => v as Expression).ToArray())
                                                    )
                                                )
                                            , typeof(object[])
                                            )
                                        )
                                    , Expression.IfThen(
                                        Expression.NotEqual(var_foreignModelQueryResult, Expression.Constant(null))
                                        , Expression.Block(
                                            GetInvokeMapModelExpressionFunction(var_foreignModel, f.ForeignModel, param_dataContext, param_customTypeConverters, var_foreignModelQueryResult, param_queryCache),
                                            GetModelPropertySetterExpressionFunction(param_m, f.FieldName, var_foreignModel)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    );

            var all_script = Expression.Block(
                all_variables
                , Expression.Assign(var_sb, Expression.New(typeof(StringBuilder)))
                , blockSimpleProperties
                , block_prep
                , blockForeignModels
                );
            
            return
                Expression.Lambda<T>(
                all_script,
                param_m,
                param_dataContext,
                param_customTypeConverters,
                param_dataRow,
                param_queryCache
                ).Compile();

            UnaryExpression GetValueConvertedExpression(Type fieldType, Type RealType, bool IsConvertible, System.Reflection.MethodInfo ParseMethod, System.Reflection.MethodInfo DateTimeOffsetParseMethod, bool IsParsable)
            {
                return
                    RealType == typeof(DateTimeOffset)
                    ? Expression.Convert(Expression.Call(DateTimeOffsetParseMethod, Expression.Call(var_value, "ToString", null, null), Expression.Constant(null, typeof(IFormatProvider)), Expression.Constant(DateTimeStyles.AssumeUniversal)), fieldType)
                    : RealType == typeof(Boolean)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToBoolean), null, var_value), fieldType)
                    : RealType == typeof(Byte)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToByte), null, var_value), fieldType)
                    : RealType == typeof(Int16)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToInt16), null, var_value), fieldType)
                    : RealType == typeof(Int32)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToInt32), null, var_value), fieldType)
                    : RealType == typeof(Int64)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToInt64), null, var_value), fieldType)
                    : RealType == typeof(SByte)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToSByte), null, var_value), fieldType)
                    : RealType == typeof(UInt16)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToUInt16), null, var_value), fieldType)
                    : RealType == typeof(UInt32)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToUInt32), null, var_value), fieldType)
                    : RealType == typeof(UInt64)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToUInt64), null, var_value), fieldType)
                    : RealType == typeof(Single)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToSingle), null, var_value), fieldType)
                    : RealType == typeof(Double)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToDouble), null, var_value), fieldType)
                    : RealType == typeof(Decimal)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToDecimal), null, var_value), fieldType)
                    : RealType == typeof(String)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToString), null, var_value), fieldType)
                    : RealType == typeof(Char)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToChar), null, var_value), fieldType)
                    : RealType == typeof(DateTime)
                    ? Expression.Convert(Expression.Call(typeof(Convert), nameof(Convert.ToDateTime), null, var_value), fieldType)
                    : IsConvertible
                    ? Expression.Convert(Expression.Call(typeof(Convert), "ChangeType", null, var_value, Expression.Constant(RealType)), fieldType)
                    : IsParsable
                    ? Expression.Convert(Expression.Call(ParseMethod, Expression.Call(var_value, "ToString", null, null)), fieldType)
                    : Expression.Convert(var_value, fieldType);
            }
        }
    }
}

