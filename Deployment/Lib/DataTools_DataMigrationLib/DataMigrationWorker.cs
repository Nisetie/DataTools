using DataTools.Attributes;
using DataTools.Common;
using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.MSSQL;
using DataTools.PostgreSQL;
using DataTools.SQLite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime;

namespace DataTools.Deploy
{

    public enum DataMigrationMode
    {
        all, create_schema, only_data
    }

    public class DataMigrationOptions
    {
        public DataMigrationMode Mode { get; set; }
        public E_DBMS FromDBMS { get; set; }
        public E_DBMS ToDBMS { get; set; }
        public string FromConnectionString { get; set; }
        public string ToConnectionString { get; set; }
        public IEnumerable<IModelMetadata> Metadatas { get; set; }

        /// <summary>
        /// При переносе данных в операции INSERT игнорировать такие ограничения, как автоинкремент или вычисляемый столбец.
        /// Иначе такие колонки будут пропущены.
        /// </summary>
        public bool IgnoreConstraints { get; set; }
        public int RowsPerBatch { get; set; } = 1000;
        public string SchemaIncludeNameFilter { get; set; } = "";
        public string TableIncludeNameFilter { get; set; } = "";
        public string SchemaExcludeNameFilter { get; set; } = "";
        public string TableExcludeNameFilter { get; set; } = "";
        public string TableIncludeNameRegexFilter { get; set; } = "";
        public string TableExcludeNameRegexFilter { get; set; } = "";
        public string SchemaIncludeNameRegexFilter { get; set; } = "";
        public string SchemaExcludeNameRegexFilter { get; set; } = "";
    }

    public class DataMigrationWorker : DataMigrationOptions
    {
        private IDataContext _fromContext, _toContext;

        private MigratorBase _toMigrator;

        public DataMigrationWorker(DataMigrationOptions options)
        {
            this.FromDBMS = options.FromDBMS;
            this.ToDBMS = options.ToDBMS;
            this.FromConnectionString = options.FromConnectionString;
            this.ToConnectionString = options.ToConnectionString;
            this.Metadatas = options.Metadatas;
            this.IgnoreConstraints = options.IgnoreConstraints;
            this.RowsPerBatch = options.RowsPerBatch;
            this.Mode = options.Mode;
            this.SchemaIncludeNameFilter = options.SchemaIncludeNameFilter;
            this.SchemaExcludeNameFilter = options.SchemaExcludeNameFilter;
            this.TableIncludeNameFilter = options.TableIncludeNameFilter;
            this.TableExcludeNameFilter = options.TableExcludeNameFilter;
            this.TableExcludeNameRegexFilter = options.TableExcludeNameRegexFilter;
            this.TableIncludeNameRegexFilter = options.TableIncludeNameRegexFilter;
            this.SchemaExcludeNameRegexFilter = options.SchemaExcludeNameRegexFilter;
            this.SchemaIncludeNameRegexFilter = options.SchemaIncludeNameRegexFilter;

            switch (FromDBMS)
            {
                case E_DBMS.MSSQL: _fromContext = new MSSQL_DataContext(FromConnectionString); break;
                case E_DBMS.PostgreSQL: _fromContext = new PostgreSQL_DataContext(FromConnectionString); break;
                case E_DBMS.SQLite: _fromContext = new SQLite_DataContext(FromConnectionString); break;
            }

            switch (ToDBMS)
            {
                case E_DBMS.MSSQL:
                    _toContext = new MSSQL_DataContext(ToConnectionString);
                    _toMigrator = new MSSQL_Migrator();
                    break;
                case E_DBMS.PostgreSQL:
                    _toContext = new PostgreSQL_DataContext(ToConnectionString);
                    _toMigrator = new PostgreSQL_Migrator();
                    break;
                case E_DBMS.SQLite:
                    _toContext = new SQLite_DataContext(ToConnectionString);
                    _toMigrator = new SQLite_Migrator();
                    break;
            }
        }

        public void Run()
        {
            RunProgress().Last();
        }

        public enum E_MIGRATION_PROGRESS
        {
            PREPARING, MIGRATING, FINALIZING
        }

        public class MigrationInfo
        {
            public E_MIGRATION_PROGRESS Progress;
            public IModelMetadata Metadata;
            public long TotalRows;
            public long InsertedRows;
        }

        public IEnumerable<MigrationInfo> RunProgress()
        {
            if (Metadatas == null)
            {
                var generator = new GeneratorWorker(new GeneratorOptions()
                {
                    ConnectionString = this.FromConnectionString,
                    DBMS = this.FromDBMS,
                    SchemaExcludeNameFilter = this.SchemaExcludeNameFilter,
                    SchemaIncludeNameFilter = this.SchemaIncludeNameFilter,
                    TableExcludeNameFilter = this.TableExcludeNameFilter,
                    TableIncludeNameFilter = this.TableIncludeNameFilter,
                    TableIncludeNameRegexFilter = this.TableIncludeNameRegexFilter,
                    TableExcludeNameRegexFilter = this.TableExcludeNameRegexFilter,
                    SchemaExcludeNameRegexFilter = this.SchemaExcludeNameRegexFilter,
                    SchemaIncludeNameRegexFilter = this.SchemaIncludeNameRegexFilter
                });
                Metadatas = generator.GetModelDefinitions().Select(md => md.ModelMetadata).ToArray();
            }

            if (Mode == DataMigrationMode.all || Mode == DataMigrationMode.create_schema)
            {
                var deployer = new DeployerWorker(new DeployerOptions()
                {
                    ConnectionString = this.ToConnectionString,
                    DBMS = this.ToDBMS,
                    Metadatas = Metadatas,
                    Mode = E_DEPLOY_MODE.REDEPLOY
                });
                deployer.Run();
            }

            if (!(Mode == DataMigrationMode.all || Mode == DataMigrationMode.only_data))
                yield break;

            var metas = MetadataHelper.SortForUndeploy(Metadatas).ToArray();
            foreach (var meta in metas)
            {
                if (meta.IsView) continue;
                _toContext.Execute(_toMigrator.GetClearTableQuery(meta));
            }

            metas = MetadataHelper.SortForDeploy(Metadatas).ToArray();
            foreach (var meta in metas)
            {
                if (meta.IsView) continue;

                yield return new MigrationInfo() { Progress = E_MIGRATION_PROGRESS.PREPARING, Metadata = meta };
                var queryBefore = _toMigrator.BeforeMigration(meta);
                if (!string.IsNullOrEmpty(queryBefore.ToString()))
                    _toContext.Execute(queryBefore);

                var fromMeta = meta;
                IModelMetadata toMeta = null;

                if (IgnoreConstraints)
                {
                    toMeta = meta.Copy();
                    var fields = toMeta.Fields.ToArray();
                    foreach (var field in fields)
                    {
                        field.IgnoreChanges = false;
                        field.IsUnique = false;
                        field.IsPrimaryKey = false;
                        field.IsAutoincrement = false;
                    }
                }
                else toMeta = meta;

                long count = 0;

                var selectCount = new SqlSelect().From(meta.FullObjectName).Select(new SqlCustom("count(*)"));

                var scalarResult = _fromContext.ExecuteScalar(selectCount);
                if (scalarResult is int)
                    count = (int)scalarResult;
                else if (scalarResult is long)
                    count = (long)scalarResult;

                long startBound = 0;
                long rowsPerPage = RowsPerBatch;
                long pages = count / rowsPerPage;

                var offset = new SqlConstant(startBound);

                var selectQuery = new SqlSelect()
                    .From(meta)
                    .Select(meta)
                    .Limit(new SqlConstant(rowsPerPage))
                    .Offset(offset);

                selectQuery.OrderBy(DataTools.Meta.MetadataHelper.GetOrderClausesFromColumnMetas(meta.GetColumnsForFilterOrder()));

                var insertBatch = new SqlInsertBatch().Into(toMeta);
                var results = _fromContext.ExecuteWithResult(selectQuery).ToArray();
                while (results.Length > 0)
                {
                    insertBatch.Value(toMeta, results);
                    _toContext.Execute(new SqlComposition(queryBefore, insertBatch));

                    startBound += rowsPerPage;
                    yield return new MigrationInfo() { Progress = E_MIGRATION_PROGRESS.MIGRATING, Metadata = meta, TotalRows = count, InsertedRows = startBound < count ? startBound : count };

                    offset.Value = startBound;
                    results = _fromContext.ExecuteWithResult(selectQuery).ToArray();

                    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Forced, true, true);
                    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.Default;
                }

                yield return new MigrationInfo() { Progress = E_MIGRATION_PROGRESS.FINALIZING, Metadata = meta };
                var queryAfter = _toMigrator.AfterMigration(meta);
                if (!string.IsNullOrEmpty(queryAfter.ToString()))
                    _toContext.Execute(queryAfter);
            }
        }
    }
}
