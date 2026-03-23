using DataTools.DML;
using DataTools.Interfaces;
using System;

namespace DataTools.Deploy
{
    public class PostgreSQL_Migrator : MigratorBase
    {
        private static readonly ISqlExpression _emptyQuery = new SqlCustom();
        private static readonly string _newLine = Environment.NewLine;

        private IDataContext _dataContext = null;
        private IModelMetadata _modelMetadata = null;
        private bool _isExtensionObject = false;
        private string _extensionName = null;
        private ISqlExpression _queryBefore = _emptyQuery;
        private ISqlExpression _queryAfter = _emptyQuery;
        private bool _RunBeforeMigrationWasExecutedOnce = false;
        public override void SetupModel(IDataContext dataContext, IModelMetadata modelMetadata)
        {
            _dataContext = dataContext;
            _modelMetadata = modelMetadata;
            _RunBeforeMigrationWasExecutedOnce = false;

            var query = new SqlComposition(
                new SqlCustom(@"
select e.extname from pg_class c
join pg_depend d on c.oid = d.objid
join pg_extension e on d.refobjid = e.oid
where c.oid = 
"),
new SqlConstant(_modelMetadata.FullObjectName),
new SqlCustom("::regclass")
                );
            _extensionName = (string)_dataContext.ExecuteScalar(query);
            _isExtensionObject = !string.IsNullOrEmpty(_extensionName);


            PrepareBeforeQuery();
            PrepareAfterQuery();
        }

        private void PrepareBeforeQuery()
        {
            IModelFieldMetadata identityColumn = null;
            foreach (var f in _modelMetadata.Fields)
            {
                if (f.IsAutoincrement)
                {
                    identityColumn = f;
                    break;
                }
            }
            if (identityColumn == null) return;

            _queryBefore = new SqlComposition(
                new SqlCustom($"DO $${_newLine}"),
                new SqlCustom($"DECLARE{_newLine}"),
                new SqlCustom($"seq_name text;{_newLine}"),
                new SqlCustom($"BEGIN{_newLine}"),
                new SqlCustom($"SELECT pg_get_serial_sequence('"),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom($"', '"),
                new SqlName(identityColumn.ColumnName),
                new SqlCustom($"') INTO seq_name;{_newLine}"),
                _isExtensionObject ? new SqlCustom($"alter extension  {_extensionName} drop table {_modelMetadata.FullObjectName};{_newLine}") : new SqlCustom(),
                _isExtensionObject ? new SqlCustom($"execute format('alter extension {_extensionName} drop sequence %s;',seq_name);{_newLine}") : new SqlCustom(),
                new SqlCustom($"ALTER TABLE "),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom($" ALTER COLUMN "),
                new SqlName(identityColumn.ColumnName),
                new SqlCustom($" DROP IDENTITY IF EXISTS;{_newLine}"),
                new SqlCustom($"END$$;")
   );
        }

        private void PrepareAfterQuery()
        {
            IModelFieldMetadata identityColumn = null;
            foreach (var f in _modelMetadata.Fields)
            {
                if (f.IsAutoincrement)
                {
                    identityColumn = f;
                    break;
                }
            }
            if (identityColumn == null) return;

            // восстановить нумерацию автоинкремента
            var composition = new SqlComposition(
                new SqlCustom($"DO $${_newLine}"),
                new SqlCustom($"DECLARE{_newLine}"),
                new SqlCustom($"seq_name text;{_newLine}"),
                new SqlCustom($"max_val bigint;{_newLine}"),
                new SqlCustom($"BEGIN{_newLine}"),
                new SqlCustom($"SELECT pg_get_serial_sequence('"),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom($"', '"),
                new SqlName(identityColumn.ColumnName),
                new SqlCustom($"') INTO seq_name;{_newLine}"),
                new SqlCustom($"SELECT coalesce(MAX("),
                new SqlName(identityColumn.ColumnName),
                new SqlCustom($"),0)+1 INTO max_val FROM "),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom($";{_newLine}"),
                new SqlCustom($"EXECUTE format('ALTER SEQUENCE %s RESTART WITH %s', seq_name, max_val);{_newLine}"),
                new SqlCustom($"END$$;")
                );

            var query = new SqlComposition(
               new SqlCustom($"ALTER TABLE "),
               new SqlName(_modelMetadata.FullObjectName),
               new SqlCustom($" ALTER COLUMN "),
               new SqlName(identityColumn.ColumnName),
               new SqlCustom($" ADD GENERATED ALWAYS AS IDENTITY;{_newLine}"),
               composition
               );

            // вернуть объекты в расширение
            if (_isExtensionObject)
            {
                query.Add(new SqlCustom($"alter extension  {_extensionName} add table {_modelMetadata.FullObjectName};"));

                composition = new SqlComposition(
                    new SqlCustom($"DO $${_newLine}"),
                    new SqlCustom($"DECLARE{_newLine}"),
                    new SqlCustom($"seq_name text;{_newLine}"),
                    new SqlCustom($"BEGIN{_newLine}"),
                    new SqlCustom($"SELECT pg_get_serial_sequence('"),
                    new SqlName(_modelMetadata.FullObjectName),
                    new SqlCustom($"', '"),
                    new SqlName(identityColumn.ColumnName),
                    new SqlCustom($"') INTO seq_name;{_newLine}"),
                    new SqlCustom($"EXECUTE format('ALTER extension {_extensionName} add sequence %s;', seq_name);{_newLine}"),
                    new SqlCustom($"END$$;")
                );

                query.Add(composition);
            }

            _queryAfter = query;
        }

        public override ISqlExpression GetClearTableQuery()
        {
            var query = new SqlComposition(
                new SqlCustom($"DELETE FROM "),
                new SqlName(_modelMetadata.FullObjectName),
                new SqlCustom(";")
                );

            return query;
        }

        public override ISqlExpression GetBeforeMigrationQuery()
        {
            // достаточно один раз удалить автоинкрементирование
            if (_RunBeforeMigrationWasExecutedOnce) return _emptyQuery;
            _RunBeforeMigrationWasExecutedOnce = true;
            return _queryBefore;
        }

        public override ISqlExpression GetAfterMigrationQuery()
        {
            if (_RunBeforeMigrationWasExecutedOnce)
                return _queryAfter;
            else
                return _emptyQuery;
        }
    }
}
