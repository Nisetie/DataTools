using DataTools.Common;
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

        public static SqlInsert Into<ModelT>(this SqlInsert sqlInsert, ModelT model) where ModelT : class, new()
        {
            Into(sqlInsert, ModelMetadata<ModelT>.Instance);
            ModelMapper<ModelT>.BindInsertValues(sqlInsert, model);
            return sqlInsert;
        }

        public static SqlInsert Into(this SqlInsert sqlInsert, IModelMetadata meta)
        {
            return sqlInsert
                .Into(meta.FullObjectName)
                .Column(meta.GetColumnsForInsertUpdate().ToArray());
        }

        public static SqlInsert Into(this SqlInsert sqlInsert, IModelMetadata meta, dynamic model)
        {
            sqlInsert
                .Into(meta.FullObjectName)
                .Column(meta.GetColumnsForInsertUpdate().ToArray());
            DynamicMapper.GetMapper(meta).BindInsertValues(sqlInsert, model);
            return sqlInsert;
        }

        public static SqlInsert Into(this SqlInsert sqlInsert, string objectName) => sqlInsert.Into(new SqlName(objectName));
        public static SqlInsert Column(this SqlInsert sqlInsert, params string[] columns) => sqlInsert.Column(columns.Select(c => new SqlName(c)).ToArray());
        public static SqlInsert Value(this SqlInsert sqlInsert, params object[] values) => sqlInsert.Value(values.Select(c => new SqlConstant(c)).ToArray());
    }
}

