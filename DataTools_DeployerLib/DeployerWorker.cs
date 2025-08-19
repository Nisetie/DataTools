using DataTools.Common;
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
    [Flags]
    public enum E_DEPLOY_MODE
    {
        DEPLOY = 1,
        UNDEPLOY = 1 << 1,
        REDEPLOY = DEPLOY | UNDEPLOY
    }

    public class DeployerOptions
    {
        public string ConnectionString { get; set; }
        public E_DEPLOY_MODE Mode { get; set; }
        public E_DBMS DBMS { get; set; }
        public bool IgnoreAllCostraints { get; set; } = false;
        public bool IgnoreIdentities { get; set; }
        public IEnumerable<IModelMetadata> Metadatas { get; set; }
    }

    public class DeployerWorker : DeployerOptions
    {
        private IDataContext _context;

        public IDataContext DataContext => _context;

        public DeployerWorker(DeployerOptions options)
        {
            this.ConnectionString = options.ConnectionString;
            this.Mode = options.Mode;
            this.DBMS = options.DBMS;
            this.IgnoreAllCostraints = options.IgnoreAllCostraints;
            this.IgnoreIdentities = options.IgnoreIdentities;
            this.Metadatas = options.Metadatas;

            switch (DBMS)
            {
                case E_DBMS.MSSQL: _context = new MSSQL_DataContext(ConnectionString); break;
                case E_DBMS.PostgreSQL: _context = new PostgreSQL_DataContext(ConnectionString); break;
                case E_DBMS.SQLite: _context = new SQLite_DataContext(ConnectionString); break;
            }
        }

        public void Run()
        {
            foreach (var _ in RunProgress()) { }
        }

        public IEnumerable<DeployInfo> RunProgress()
        {
            if ((Mode & E_DEPLOY_MODE.UNDEPLOY) == E_DEPLOY_MODE.UNDEPLOY)
                foreach (var deployInfo in ProcessUndeploy()) yield return deployInfo;

            if ((Mode & E_DEPLOY_MODE.DEPLOY) == E_DEPLOY_MODE.DEPLOY)
                foreach (var deployInfo in ProcessDeploy()) yield return deployInfo;
        }

        public class DeployInfo
        {
            public IModelMetadata Metadata;
            public E_DEPLOY_MODE Mode;
        }

        private IEnumerable<DeployInfo> ProcessDeploy()
        {
            if (IgnoreAllCostraints)
            {
                foreach (var meta in Metadatas)
                {
                    var fields = meta.Fields.ToArray();
                    foreach (var field in fields)
                        meta.RemoveField(field);
                    foreach (var field in fields)
                    {
                        field.IgnoreChanges = false;
                        field.IsUnique = false;
                        field.IsPrimaryKey = false;
                        field.IsAutoincrement = false;
                        if (field.IsForeignKey)
                        {
                            meta.RemoveField(field);
                            int i = 0;
                            foreach (var col in field.ColumnNames)
                            {
                                var newField = field.ForeignModel.GetColumn(field.ForeignColumnNames[i]).Copy();
                                newField.ColumnName = col;
                                newField.FieldName = col;
                                newField.IsUnique = false;
                                newField.IsPrimaryKey = false;
                                newField.IsAutoincrement = false;
                                meta.AddField(newField);
                            }
                        }
                        else meta.AddField(field);
                    }
                }
            }
            else if (IgnoreIdentities)
            {
                foreach (var meta in Metadatas)
                    foreach (var field in meta.Fields)
                        field.IsAutoincrement = false;
            }

            var alreadyCreated = new List<string>();

            foreach (var meta in MetadataHelper.SortForDeploy(Metadatas))
            {
                Deploy(meta);
                yield return new DeployInfo() { Metadata = meta, Mode = E_DEPLOY_MODE.DEPLOY };
            }
        }

        private IEnumerable<DeployInfo> ProcessUndeploy()
        {
            foreach (var meta in MetadataHelper.SortForUndeploy(Metadatas))
            {
                Undeploy(meta);
                yield return new DeployInfo() { Metadata = meta, Mode = E_DEPLOY_MODE.UNDEPLOY };
            }
        }

        private void Deploy(IModelMetadata metadata)
        {
            if (!metadata.IsView)
                _context.CreateTable(metadata);
        }

        private void Undeploy(IModelMetadata metadata)
        {
            if (!metadata.IsView)
                _context.DropTable(metadata);
        }
    }
}