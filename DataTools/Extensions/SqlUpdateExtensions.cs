using DataTools.DML;
using DataTools.Meta;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlUpdateExtensions
    {
        /// <summary>
        /// Возвращается подготовленная команда Update, в которой остается заполнить значения value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static SqlUpdate From<ModelT>(this SqlUpdate sqlUpdate) where ModelT : class, new()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            var fields = meta.Fields;
            var copy = sqlUpdate
                .From(meta.FullObjectName)
                .Set((from field in fields where !field.IgnoreChanges select field.ColumnName).ToArray());
            return copy;
        }
        public static SqlUpdate From(this SqlUpdate sqlUpdate, string objectName) => sqlUpdate.From(new SqlName(objectName));
        public static SqlUpdate Set(this SqlUpdate sqlUpdate, params string[] columns) => sqlUpdate.Set(columns.Select(c => new SqlName(c)).ToArray());
        public static SqlUpdate Value(this SqlUpdate sqlUpdate, params object[] values) => sqlUpdate.Value(values.Select(v => new SqlConstant(v)).ToArray());
    }
}

