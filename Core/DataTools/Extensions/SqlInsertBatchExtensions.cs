using DataTools.DDL;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlInsertBatchExtensions
    {

        /// Возвращается подготовленная команда Insert, в которой остается заполнить значения values для всех изменяемых полей <typeparamref name="ModelT"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static SqlInsertBatch Into<ModelT>(this SqlInsertBatch sqlInsertBatch) where ModelT : class, new()
        {
            return Into(sqlInsertBatch, ModelMetadata<ModelT>.Instance);
        }

        public static SqlInsertBatch Into(this SqlInsertBatch sqlInsertBatch, IModelMetadata meta)
        {
            return sqlInsertBatch
                .Into(meta.FullObjectName)
                .Column(MetadataHelper.GetColumnNamesFromColumnMetas(meta.GetColumnsForInsertUpdate()));
        }

        public static SqlInsertBatch Into(this SqlInsertBatch sqlInsertBatch, string objectName) => sqlInsertBatch.Into(new SqlName(objectName));
        public static SqlInsertBatch Column(this SqlInsertBatch sqlInsertBatch, params string[] columns)
        {
            var sqlNames = new SqlName[columns.Length];
            for (int i = 0; i < columns.Length; i++)
                sqlNames[i] = new SqlName(columns[i]);
            return sqlInsertBatch.Column(sqlNames);
        }

        /// <summary>
        /// Подготовка команды INSERT.
        /// Сопоставление sql-типов будет определяться по GetType() у каждого значения.
        /// </summary>
        /// <param name="sqlInsertBatch"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public static SqlInsertBatch Value(this SqlInsertBatch sqlInsertBatch, params object[][] values)
        {
            int valuesCount = values[0].Length;
            var sqlValues = new SqlConstant[values.Length][];
            for (int i = 0; i < values.Length; ++i)
            {
                sqlValues[i] = new SqlConstant[valuesCount];
                for (int j = 0; j < valuesCount; ++j)
                    sqlValues[i][j] = new SqlConstant(values[i][j]);
            }
            return sqlInsertBatch.Value(sqlValues);
        }

        /// <summary>
        /// Подготовка команды INSERT с учетом метаданных.
        /// Сопоставление типов будет определяться по DBType каждого поля метамодели.
        /// </summary>
        /// <param name="sqlInsertBatch"></param>
        /// <param name="modelMetadata"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public static SqlInsertBatch Value(this SqlInsertBatch sqlInsertBatch, IModelMetadata modelMetadata, params object[][] values)
        {
            var columns = new List<IModelFieldMetadata>();
            foreach (var fieldInfo in modelMetadata.GetColumnsForInsertUpdate())
            {
                if (!fieldInfo.IsForeignKey)
                    columns.Add(fieldInfo);
                else
                {
                    for (int i = 0; i < fieldInfo.ForeignColumnNames.Length; ++i)
                        columns.Add(fieldInfo.ForeignModel.GetColumn(fieldInfo.ForeignColumnNames[i]));
                }
            }
            int valuesCount = values[0].Length;
            var sqlValues = new ISqlExpression[values.Length][];
            for (int i = 0; i < values.Length; ++i)
            {
                sqlValues[i] = new ISqlExpression[valuesCount];
                for (int j = 0; j < valuesCount; ++j)
                {
                    var fieldMetadata = columns[j];
                    sqlValues[i][j] = new SqlInsertConstant(new SqlConstant(values[i][j]), fieldMetadata.ColumnDBType)
                    {
                        TextLength = fieldMetadata.TextLength,
                        NumericScale = fieldMetadata.NumericScale,
                        NumericPrecision = fieldMetadata.NumericPrecision
                    };
                }
            }
            return sqlInsertBatch.Value(sqlValues);
        }
    }
}

