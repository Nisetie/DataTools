using DataTools.Common;
using DataTools.DML;
using DataTools.Interfaces;
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
            return From(sqlUpdate, ModelMetadata<ModelT>.Instance);
        }
        public static SqlUpdate From(this SqlUpdate sqlUpdate, IModelMetadata modelMetadata)
        {
            var fields = modelMetadata.Fields;
            var copy = sqlUpdate
                .From(modelMetadata.FullObjectName)
                .Set(modelMetadata.GetColumnsForInsertUpdate().ToArray());
            return copy;
        }
        public static SqlUpdate From(this SqlUpdate sqlUpdate, string objectName) => sqlUpdate.From(new SqlName(objectName));
        public static SqlUpdate Set(this SqlUpdate sqlUpdate, params string[] columns) => sqlUpdate.Set(columns.Select(c => new SqlName(c)).ToArray());
        public static SqlUpdate Value(this SqlUpdate sqlUpdate, params object[] values) => sqlUpdate.Value(values.Select(v => new SqlConstant(v)).ToArray());

        public static SqlUpdate Value<ModelT>(this SqlUpdate sqlUpdate, ModelT model) where ModelT : class, new()
        {
         return   sqlUpdate.Value(ModelMapper<ModelT>.GetArrayOfValues(model));
        }
        public static SqlUpdate Value(this SqlUpdate sqlUpdate, IModelMetadata modelMetadata, dynamic model)
        {
            return sqlUpdate.Value((object[])DynamicMapper.GetMapper(modelMetadata).GetArrayOfValues(model));
        }
        public static SqlUpdate Where<ModelT>(this SqlUpdate sqlUpdate, ModelT model) where ModelT : class, new()
        {
            var meta = ModelMetadata<ModelT>.Instance;
            return sqlUpdate.Where(ModelMapper<ModelT>.GetWhereClause(model));
        }
        public static SqlUpdate Where(this SqlUpdate sqlUpdate, IModelMetadata modelMetadata, dynamic model)
        {
            return sqlUpdate.Where(DynamicMapper.GetMapper(modelMetadata).GetWhereClause(model));
        }
    }
}

