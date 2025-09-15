using DataTools.DML;
using DataTools.Interfaces;
using DataTools.PostgreSQL;
using System;

namespace DataTools.Deploy
{
    public class PostgreSQL_Migrator : MigratorBase
    {
        public override ISqlExpression GetClearTableQuery(IModelMetadata modelMetadata)
        {
            // в PSQL при очистке таблицы автоинкременты сбрасываются через RESTART IDENTITY
            var query = new SqlComposition(
                new SqlCustom($"DELETE FROM "),
                new SqlName(modelMetadata.FullObjectName),
                new SqlCustom(";")
                );

            return new PostgreSQL_QueryParser().SimplifyQuery(query);
        }

        public override ISqlExpression BeforeMigration(IModelMetadata modelMetadata)
        {
            IModelFieldMetadata identityColumn = null;
            foreach (var f in modelMetadata.Fields)
            {
                if (f.IsAutoincrement)
                {
                    identityColumn = f;
                    break;
                }
            }
            if (identityColumn == null) return new SqlCustom("");

            var query = new SqlComposition(
               new SqlCustom($"ALTER TABLE "),
               new SqlName(modelMetadata.FullObjectName),
               new SqlCustom($" ALTER COLUMN "),
               new SqlName(identityColumn.ColumnName),
               new SqlCustom(" DROP IDENTITY IF EXISTS;")
               );

            return new PostgreSQL_QueryParser().SimplifyQuery(query);
        }

        public override ISqlExpression AfterMigration(IModelMetadata modelMetadata)
        {
            IModelFieldMetadata identityColumn = null;
            foreach (var f in modelMetadata.Fields)
            {
                if (f.IsAutoincrement)
                {
                    identityColumn = f;
                    break;
                }
            }
            if (identityColumn == null) return new SqlCustom("");

            var composition = new SqlComposition(
                new SqlCustom($"DO $${Environment.NewLine}"),
                new SqlCustom($"DECLARE{Environment.NewLine}"),
                new SqlCustom($"seq_name text;{Environment.NewLine}"),
                new SqlCustom($"max_val bigint;{Environment.NewLine}"),
                new SqlCustom($"BEGIN{Environment.NewLine}"),
                new SqlCustom($"SELECT pg_get_serial_sequence('"),
                new SqlName(modelMetadata.FullObjectName),
                new SqlCustom($"', '"),
                new SqlName(identityColumn.ColumnName),
                new SqlCustom($"') INTO seq_name;{Environment.NewLine}"),
                new SqlCustom($"SELECT coalesce(MAX("),
                new SqlName(identityColumn.ColumnName),
                new SqlCustom($"),0)+1 INTO max_val FROM "),
                new SqlName(modelMetadata.FullObjectName),
                new SqlCustom($";{Environment.NewLine}"),
                new SqlCustom($"EXECUTE format('ALTER SEQUENCE %s RESTART WITH %s', seq_name, max_val);{Environment.NewLine}"),
                new SqlCustom($"END$$;")
                );

            var query = new SqlComposition(
               new SqlCustom($"ALTER TABLE "),
               new SqlName(modelMetadata.FullObjectName),
               new SqlCustom($" ALTER COLUMN "),
               new SqlName(identityColumn.ColumnName),
               new SqlCustom(" ADD GENERATED ALWAYS AS IDENTITY;"),
               composition
               );

            return new PostgreSQL_QueryParser().SimplifyQuery(query);
        }
    }
}
