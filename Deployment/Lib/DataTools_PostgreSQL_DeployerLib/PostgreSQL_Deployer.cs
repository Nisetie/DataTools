using DataTools.DML;
using DataTools.Interfaces;
using DataTools.PostgreSQL;
using System;

namespace DataTools.Deploy
{
    public class PostgreSQL_Deployer : DeployerBase
    {
        public override ISqlExpression GetRestoreIdentityQuery(IModelMetadata metadata)
        {
            IModelFieldMetadata field = null;
            foreach (var f in metadata.Fields)
                if (f.IsAutoincrement)
                {
                    field = f;
                    break;
                }
            if (field == null)
                return new SqlCustom("");

            var composition = new SqlComposition(
                new SqlCustom($"DO $${Environment.NewLine}"),
                new SqlCustom($"DECLARE{Environment.NewLine}"),
                new SqlCustom($"seq_name text;{Environment.NewLine}"),
                new SqlCustom($"max_val long;{Environment.NewLine}"),
                new SqlCustom($"BEGIN{Environment.NewLine}"),
                new SqlCustom($"SELECT pg_get_serial_sequence('"),
                new SqlName(metadata.FullObjectName),
                new SqlCustom($"', '"),
                new SqlName(field.ColumnName),
                new SqlCustom($"') INTO seq_name;{Environment.NewLine}"),
                new SqlCustom($"SELECT MAX("),
                new SqlName(field.ColumnName),
                new SqlCustom($")+1 INTO max_val FROM "),
                new SqlName(metadata.FullObjectName),
                new SqlCustom($";{Environment.NewLine}"),
                new SqlCustom($"EXECUTE format('ALTER SEQUENCE %I RESTART WITH %s', seq_name, max_val);{Environment.NewLine}"),
                new SqlCustom($"END$$;")
                );

            return new PostgreSQL_QueryParser().SimplifyQuery(composition);
        }
    }
}
