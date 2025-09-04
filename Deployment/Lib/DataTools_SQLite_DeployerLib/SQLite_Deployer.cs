using DataTools.DML;
using DataTools.Interfaces;
using DataTools.SQLite;
using System;

namespace DataTools.Deploy
{
    public class SQLite_Deployer : DeployerBase
    {
        public override SqlExpression GetRestoreIdentityQuery(IModelMetadata metadata)
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
                new SqlCustom($"UPDATE SQLITE_SEQUENCE{Environment.NewLine}"),
                new SqlCustom($"SET seq = (SELECT MAX("),
                new SqlName(field.ColumnName),
                new SqlCustom($") + 1 FROM "),
                new SqlName(metadata.FullObjectName),
                new SqlCustom($"){Environment.NewLine}"),
                new SqlCustom($"WHERE name = '"),
                new SqlName(metadata.FullObjectName),
                new SqlCustom($"';")
                );

            return new SQLite_QueryParser().SimplifyQuery(composition);
        }
    }
}
