using DataTools.DML;
using DataTools.Interfaces;
using DataTools.MSSQL;
using System;

namespace DataTools.Deploy
{
    public class MSSQL_Deployer : DeployerBase
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
                new SqlCustom($"DECLARE @maxValue INT;{Environment.NewLine}"),
                new SqlCustom($"SELECT @maxValue = MAX("),
                new SqlName(field.ColumnName),
                new SqlCustom($") + 1 FROM "),
                new SqlName(metadata.FullObjectName),
                new SqlCustom($";{Environment.NewLine}"),
                new SqlCustom($"DBCC CHECKIDENT ('"),
                new SqlName(metadata.FullObjectName),
                new SqlCustom($"', RESEED, @maxValue);")
                );

            return new MSSQL_QueryParser().SimplifyQuery(composition);
        }
    }
}
