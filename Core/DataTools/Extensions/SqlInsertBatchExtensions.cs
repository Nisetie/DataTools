using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
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

