using DataTools.DML;
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
            var meta = ModelMetadata<ModelT>.Instance;
            var fields = meta.Fields;
            var copy = sqlInsert
                .Into(meta.FullObjectName)
                .Column((from field in fields where !field.IgnoreChanges select field.ColumnName).ToArray());
            return copy;
        }
        public static SqlInsert Into(this SqlInsert sqlInsert, string objectName) => sqlInsert.Into(new SqlName(objectName));
        public static SqlInsert Column(this SqlInsert sqlInsert, params string[] columns) => sqlInsert.Column(columns.Select(c => new SqlName(c)).ToArray());
        public static SqlInsert Value(this SqlInsert sqlInsert, params object[] values) => sqlInsert.Value(values.Select(c => new SqlConstant(c)).ToArray());
    }
}

