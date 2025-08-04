using DataTools.DML;
using DataTools.Interfaces;
using DataTools.SQLite;

namespace DataTools.Deploy
{
    public class SQLite_Migrator : MigratorBase
    {
        public override SqlExpression GetClearTableQuery(IModelMetadata modelMetadata)
        {
            // в SQLite нет TRUNCATE
            var query = new SqlComposition(
                new SqlCustom($"DELETE FROM "),
                new SqlName(modelMetadata.FullObjectName),
                new SqlCustom(";")
                );

            return new SQLite_QueryParser().SimplifyQuery(query);
        }

        public override SqlExpression BeforeMigration(IModelMetadata modelMetadata)
        {
            return new SqlCustom("");
        }

        public override SqlExpression AfterMigration(IModelMetadata modelMetadata)
        {
            return new SqlCustom("");
        }
    }
}
