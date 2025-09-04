using DataTools.DML;
using DataTools.Interfaces;
using DataTools.Meta;

namespace DataTools.Extensions
{
    public static class SqlDeleteExtensions
    {
        public static SqlDelete From<ModelT>(this SqlDelete sqlDelete) where ModelT : class, new() => From(sqlDelete, ModelMetadata<ModelT>.Instance.FullObjectName);
        public static SqlDelete From(this SqlDelete sqlDelete, IModelMetadata modelMetadata) => sqlDelete.From(modelMetadata.FullObjectName);
        public static SqlDelete From(this SqlDelete sqlDelete, string objectName) => sqlDelete.From(new SqlName(objectName));
        public static SqlDelete Where(this SqlDelete sqlDelete, string columnName, object value) => sqlDelete.Where(new SqlWhere().Name(columnName).EqValue(value));
    }

}

