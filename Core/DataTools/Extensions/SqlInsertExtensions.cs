using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlInsertExtensions
    {
        /// <summary>
        /// Заполнить имя таблицы вставки и перечень колонок для вставки.
        /// </summary>
        /// <typeparam name="ModelT"></typeparam>
        /// <param name="sqlInsert"></param>
        /// <returns></returns>
        public static SqlInsert Into<ModelT>(this SqlInsert sqlInsert) where ModelT : class, new()
        {
            return Into(sqlInsert, ModelMetadata<ModelT>.Instance);
        }

        /// <summary>
        /// Заполнить имя таблицы вставки и перечень колонок для вставки.
        /// </summary>
        /// <param name="sqlInsert"></param>
        /// <param name="meta"></param>
        /// <returns></returns>
        public static SqlInsert Into(this SqlInsert sqlInsert, IModelMetadata meta)
        {
            return sqlInsert
                .Into(meta.FullObjectName)
                .Column(meta.GetColumnsForInsertUpdate().ToArray());
        }
        public static SqlInsert Value<ModelT>(this SqlInsert sqlInsert, ModelT model) where ModelT : class, new()
        {
            return sqlInsert.Value((object[])ModelMapper<ModelT>.GetArrayOfValues(model));
        }
        public static SqlInsert Value(this SqlInsert sqlInsert, IModelMetadata meta, dynamic model)
        {
            return sqlInsert.Value(DynamicMapper.GetMapper(meta).GetArrayOfValues(model));
        }

        public static SqlInsert Into(this SqlInsert sqlInsert, string objectName) => sqlInsert.Into(new SqlName(objectName));
        public static SqlInsert Column(this SqlInsert sqlInsert, params string[] columns)
        {
            return sqlInsert.Column(columns.Select(c => new SqlName(c)).ToArray());
        }

        public static SqlInsert Value(this SqlInsert sqlInsert, params object[] values)
        {
            return sqlInsert.Value(values.Select(c => new SqlConstant(c)).ToArray());
        }
    }
}

