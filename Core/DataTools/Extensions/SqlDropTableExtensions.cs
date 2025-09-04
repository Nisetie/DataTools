using DataTools.DDL;
using DataTools.Interfaces;
using DataTools.Meta;

namespace DataTools.Extensions
{
    public static class SqlDropTableExtensions
    {
        public static SqlDropTable Table(this SqlDropTable sqlDropTable, string tableName)
        {
            return sqlDropTable.Table(new DML.SqlName(tableName));
        }

        public static SqlDropTable Table(this SqlDropTable sqlDropTable, IModelMetadata modelMetadata)
        {
            sqlDropTable.Table(modelMetadata.FullObjectName);
            return sqlDropTable;
        }

        public static SqlDropTable Table<ModelT>(this SqlDropTable sqlDropTable) where ModelT : class, new()
        {
            return Table(sqlDropTable, ModelMetadata<ModelT>.Instance);
        }
    }
}