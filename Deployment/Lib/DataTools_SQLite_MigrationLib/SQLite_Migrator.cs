using DataTools.DML;
using DataTools.Interfaces;
using DataTools.SQLite;

namespace DataTools.Deploy
{
    public class SQLite_Migrator : MigratorBase
    {
        private IDataContext _dataContext = null;
        private IModelMetadata _modelMetadata = null;
        public override void SetupModel(IDataContext dataContext, IModelMetadata modelMetadata)
        {
            _dataContext = dataContext;
            _modelMetadata = modelMetadata;
        }

        public override ISqlExpression GetClearTableQuery()
        {
            // в SQLite нет TRUNCATE
            var query = new SqlComposition(
                new SqlCustom($"DELETE FROM "),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom(";")
                );

            return query;
        }

        public override ISqlExpression GetBeforeMigrationQuery()
        {
            return new SqlCustom();
        }

        public override ISqlExpression GetAfterMigrationQuery()
        {
            return new SqlCustom();
        }
    }
}
