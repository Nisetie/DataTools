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

namespace DataTools.Deploy
{

    public class DataMigrationOptions
    {
        public E_DBMS FromDBMS { get; set; }
        public E_DBMS ToDBMS { get; set; }
        public string FromConnectionString { get; set; }
        public string ToConnectionString { get; set; }
        public IEnumerable<IModelMetadata> Metadatas { get; set; }
        public bool IgnoreConstraints { get; set; }
        public int RowsPerBatch { get; set; } = 1000;
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
            foreach (var _ in RunProgress()) { }            
        }

        public enum E_MIGRATION_PROGRESS { BEFORE, MIGRATING, AFTER }

        public class MigrationInfo
        {
            public E_MIGRATION_PROGRESS Progress;
            public IModelMetadata Metadata;
            public long TotalRows;
            public long InsertedRows;
        }

        public IEnumerable<MigrationInfo> RunProgress()
        {
            var metas = MetadataHelper.SortForUndeploy(Metadatas).ToArray();
            foreach (var meta in metas)
            {
                //yield return new MigrationInfo() { Progress = E_MIGRATION_PROGRESS.BEFORE, Metadata = meta };
                _toContext.Execute(_toMigrator.GetClearTableQuery(meta));
            }

            metas = MetadataHelper.SortForDeploy(Metadatas).ToArray();
            foreach (var meta in metas)
            {
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

                var orderedFields = meta.GetColumnsForOrdering().ToArray();

                var offset = new SqlConstant(startBound);

                var selectQuery = new SqlSelect()
                    .From(meta)
                    .Limit(new SqlConstant(rowsPerPage))
                    .Offset(offset);
                if (orderedFields != null && orderedFields.Length > 0)
                    selectQuery.OrderBy(orderedFields);

                var insertBatch = new SqlInsertBatch().Into(toMeta);
                var results = _fromContext.ExecuteWithResult(selectQuery).ToArray();
                while (results.Length > 0)
                {
                    insertBatch.Value(results);
                    _toContext.Execute(new SqlComposition(queryBefore,insertBatch));

                    startBound += rowsPerPage;
                    yield return new MigrationInfo() { Progress = E_MIGRATION_PROGRESS.MIGRATING, Metadata = meta, TotalRows = count, InsertedRows = startBound < count ? startBound : count };

                    offset.Value = startBound;
                    results = _fromContext.ExecuteWithResult(selectQuery).ToArray();

                    GC.Collect(2, GCCollectionMode.Forced, true, true);
                }


                //yield return new MigrationInfo() { Progress = E_MIGRATION_PROGRESS.AFTER, Metadata = meta };

                var queryAfter = _toMigrator.AfterMigration(meta);
                if (!string.IsNullOrEmpty(queryAfter.ToString()))
                    _toContext.Execute(queryAfter);
            }
        }
    }
}
