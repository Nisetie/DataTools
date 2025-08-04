using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlInsertExtensions
    {

        /// Возвращается подготовленная команда Insert, в которой остается заполнить значения values для всех изменяемых полей <typeparamref name="ModelT"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static SqlInsert Into<ModelT>(this SqlInsert sqlInsert) where ModelT : class, new()
        {
            return Into(sqlInsert, ModelMetadata<ModelT>.Instance);
        }

        public static SqlInsert Into(this SqlInsert sqlInsert, IModelMetadata meta)
        {
            return sqlInsert
                .Into(meta.FullObjectName)
                .Column(meta.GetColumnsForInsertUpdate().ToArray());
        }

        public static SqlInsert Into(this SqlInsert sqlInsert, string objectName) => sqlInsert.Into(new SqlName(objectName));
        public static SqlInsert Column(this SqlInsert sqlInsert, params string[] columns) => sqlInsert.Column(columns.Select(c => new SqlName(c)).ToArray());
        public static SqlInsert Value(this SqlInsert sqlInsert, params object[] values) => sqlInsert.Value(values.Select(c => new SqlConstant(c)).ToArray());
    }

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
                .Column(meta.GetColumnsForInsertUpdate().ToArray());
        }

        public static SqlInsertBatch Into(this SqlInsertBatch sqlInsertBatch, string objectName) => sqlInsertBatch.Into(new SqlName(objectName));
        public static SqlInsertBatch Column(this SqlInsertBatch sqlInsertBatch, params string[] columns)
        {
            var sqlNames = new SqlName[columns.Length];
            for (int i = 0; i < columns.Length; i++)
                sqlNames[i] = new SqlName(columns[i]);
            return sqlInsertBatch.Column(sqlNames);
        }

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
    }
}

