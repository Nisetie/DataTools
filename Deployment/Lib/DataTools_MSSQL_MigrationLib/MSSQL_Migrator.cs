using DataTools.DML;
using DataTools.Interfaces;
using DataTools.MSSQL;
using System;

namespace DataTools.Deploy
{
    public class MSSQL_Migrator : MigratorBase
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
            // TRUNCATE не работает, если у таблицы есть внешние связи. Стоит ли ради TRUNCATE писать логику их удаления и восстановления? Не думаю...
            var query = new SqlComposition(
               new SqlCustom($"DELETE FROM "),
               new SqlName(_modelMetadata.FullObjectName),
               new SqlCustom(";")
               );

            return query;
        }

        public override ISqlExpression GetBeforeMigrationQuery()
        {
            IModelFieldMetadata field = null;
            foreach (var f in _modelMetadata.Fields)
                if (f.IsAutoincrement)
                {
                    field = f;
                    break;
                }
            if (field == null)
                return new SqlCustom();

            var query = new SqlComposition(
               new SqlCustom($"SET IDENTITY_INSERT "),
               new SqlName(_modelMetadata.FullObjectName),
               new SqlCustom(" ON;")
               );

            return query;
        }

        public override ISqlExpression GetAfterMigrationQuery()
        {
            IModelFieldMetadata field = null;
            foreach (var f in _modelMetadata.Fields)
                if (f.IsAutoincrement)
                {
                    field = f;
                    break;
                }
            if (field == null)
                return new SqlCustom();

            var composition = new SqlComposition(
                new SqlCustom($"DECLARE @maxValue INT;{Environment.NewLine}"),
                new SqlCustom($"SELECT @maxValue = coalesce(MAX("),
                new SqlName(field.ColumnName),
                new SqlCustom($"),0) + 1 FROM "),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom($";{Environment.NewLine}"),
                new SqlCustom($"DBCC CHECKIDENT ('"),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom($"', RESEED, @maxValue);")
                );

            var query = new SqlComposition(
               new SqlCustom($"SET IDENTITY_INSERT "),
               new SqlName(_modelMetadata.FullObjectName),
               new SqlCustom(" OFF;"),
               composition
               );

            return query;
        }
    }
}
