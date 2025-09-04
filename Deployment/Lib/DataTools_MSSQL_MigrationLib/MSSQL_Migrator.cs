using DataTools.DML;
using DataTools.Interfaces;
using DataTools.MSSQL;
using System;

namespace DataTools.Deploy
{
    public class MSSQL_Migrator : MigratorBase
    {
        public override SqlExpression GetClearTableQuery(IModelMetadata modelMetadata)
        {
            var query = new SqlComposition(
               new SqlCustom($"DELETE FROM "),
               new SqlName(modelMetadata.FullObjectName),
               new SqlCustom(";")
               );

            return new MSSQL_QueryParser().SimplifyQuery(query);
        }

        public override SqlExpression BeforeMigration(IModelMetadata modelMetadata)
        {
            IModelFieldMetadata field = null;
            foreach (var f in modelMetadata.Fields)
                if (f.IsAutoincrement)
                {
                    field = f;
                    break;
                }
            if (field == null)
                return new SqlCustom("");

            var query = new SqlComposition(
               new SqlCustom($"SET IDENTITY_INSERT "),
               new SqlName(modelMetadata.FullObjectName),
               new SqlCustom(" ON;")
               );

            return new MSSQL_QueryParser().SimplifyQuery(query);
        }

        public override SqlExpression AfterMigration(IModelMetadata modelMetadata)
        {
            IModelFieldMetadata field = null;
            foreach (var f in modelMetadata.Fields)
                if (f.IsAutoincrement)
                {
                    field = f;
                    break;
                }
            if (field == null)
                return new SqlCustom("");

            var composition = new SqlComposition(
                new SqlCustom($"DECLARE @maxValue INT;{Environment.NewLine}"),
                new SqlCustom($"SELECT @maxValue = coalesce(MAX("),
                new SqlName(field.ColumnName),
                new SqlCustom($"),0) + 1 FROM "),
                new SqlName(modelMetadata.FullObjectName),
                new SqlCustom($";{Environment.NewLine}"),
                new SqlCustom($"DBCC CHECKIDENT ('"),
                new SqlName(modelMetadata.FullObjectName),
                new SqlCustom($"', RESEED, @maxValue);")
                );

            var query = new SqlComposition(
               new SqlCustom($"SET IDENTITY_INSERT "),
               new SqlName(modelMetadata.FullObjectName),
               new SqlCustom(" OFF;"),
               composition
               );

            return new MSSQL_QueryParser().SimplifyQuery(query);
        }
    }
}
