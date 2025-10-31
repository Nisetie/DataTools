using DataTools.Common;
using DataTools.Deploy;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Reflection;
using System.Text.Json;
using Tools.InputArguments;

namespace DataTools_DataMigration
{
    internal class Program
    {
        private static ArgumentsCollection _arguments;

        private static string _metamodelsFileName;
        private static string _assemblyFileName;
        private static Assembly _assembly;

        private static E_DBMS _fromdbms;
        private static E_DBMS _todbms;

        private static string _fromConnectionString;
        private static string _toConnectionString;

        private static bool _verbose = false;

        private static Dictionary<string, E_DBMS> _dbmsKeys;
        private static Dictionary<string, DataMigrationMode> _modes;

        private static bool _ignoreCostraints = false;

        private static bool _debug = false;

        private static int _rowsPerBatch = 1000;

        private static DataMigrationMode _mode = DataMigrationMode.only_data;
        private static string _schemaNameIncludeFilter = "";
        private static string _tableNameIncludeFilter = "";
        private static string _schemaNameExcludeFilter = "";
        private static string _tableNameExcludeFilter = "";
        private static string _schemaNameIncludeRegexFilter = "";
        private static string _tableNameIncludeRegexFilter = "";
        private static string _schemaNameExcludeRegexFilter = "";
        private static string _tableNameExcludeRegexFilter = "";

        private static DataMigrationWorker _worker;

        static Program()
        {
            _dbmsKeys = new Dictionary<string, E_DBMS>();
            var names = Enum.GetNames<E_DBMS>();
            foreach (var name in names)
            {
                _dbmsKeys[name.ToLower()] = Enum.Parse<E_DBMS>(name);
            }

            _modes = new Dictionary<string, DataMigrationMode>();
            var names1 = Enum.GetNames<DataMigrationMode>();
            foreach (var name in names1)
            {
                _modes[name.ToLower()] = Enum.Parse<DataMigrationMode>(name);
            }
        }

        private static void ConsoleWriteLine(string message)
        {
            Console.WriteLine($"[{DateTime.Now:o}] {message}");
        }

        private static void Main(string[] args)
        {
            string processCatalog = Path.GetDirectoryName(Environment.ProcessPath);

            _arguments = new ArgumentsCollection();


            _arguments.AddParameter(new InputArgumentWithInput("-a", "Assembly file path.", (string path) => _assemblyFileName = Path.GetFullPath(path, processCatalog)), false, "-m");
            _arguments.AddParameter(new InputArgumentWithInput("-m", "Metamodels file path.", (string path) => _metamodelsFileName = Path.GetFullPath(path, processCatalog)), false, "-a");
            _arguments.AddParameter(new InputArgumentWithInput("-fd", $"From DBMS product: {string.Join(',', Enum.GetNames<E_DBMS>().Select(n => n.ToLower()))}.", (string dbms) =>
            {
                _fromdbms = _dbmsKeys[dbms];
            }), true);
            _arguments.AddParameter(new InputArgumentWithInput("-td", $"To DBMS product: {string.Join(',', Enum.GetNames<E_DBMS>().Select(n => n.ToLower()))}.", (string dbms) =>
            {
                _todbms = _dbmsKeys[dbms];
            }), true);
            _arguments.AddParameter(new InputArgumentWithInput("-fc", "From connection string", (string cs) => { _fromConnectionString = cs; }), true, "-ff");
            _arguments.AddParameter(new InputArgumentWithInput("-ff", "From filename with connection string", (string filename) =>
            {
                string cs = string.Empty;
                try
                {
                    cs = File.ReadLines(Path.GetFullPath(filename, processCatalog)).FirstOrDefault();
                    if (!string.IsNullOrEmpty(cs))
                    {
                        _fromConnectionString = cs;
                    }
                    else throw new Exception();
                }
                catch (Exception e)
                {
                    ConsoleWriteLine($"Error reading {cs}. {e.Message}");
                    _arguments.ShowHelp(1);
                }
            }), true, "-fc");
            _arguments.AddParameter(new InputArgumentWithInput("-tc", "To connection string", (string cs) => { _toConnectionString = cs; }), true, "-tf");
            _arguments.AddParameter(new InputArgumentWithInput("-tf", "To filename with connection string", (string filename) =>
            {
                string cs = string.Empty;
                try
                {
                    cs = File.ReadLines(Path.GetFullPath(filename, processCatalog)).FirstOrDefault();
                    if (!string.IsNullOrEmpty(cs))
                    {
                        _toConnectionString = cs;
                    }
                    else throw new Exception();
                }
                catch (Exception e)
                {
                    ConsoleWriteLine($"Error reading {cs}. {e.Message}");
                    _arguments.ShowHelp(1);
                }
            }), true, "-tc");
            _arguments.AddParameter(new InputArgumentWithInput("-mm", $"Migration mode: {string.Join(',', Enum.GetNames<DataMigrationMode>().Select(n => n.ToLower()))}. Default: -mm {_mode.ToString().ToLower()}", (string mode) =>
            {
                _mode = _modes[mode];
            }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-s", "Schema name include filter (ex. 'dbo')", (string schemaFilter) => { _schemaNameIncludeFilter = schemaFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-sr", "Schema name include regex filter (ex. 'dbo')", (string schemaFilter) => { _schemaNameIncludeRegexFilter = schemaFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-t", "Table name include filter (ex. 'Student')", (string tableFilter) => { _tableNameIncludeFilter = tableFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-tr", "Table name include regex filter (ex. 'Student')", (string tableFilter) => { _tableNameIncludeRegexFilter = tableFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-sx", "Schema name exclude filter (ex. 'dbo')", (string schemaFilter) => { _schemaNameExcludeFilter = schemaFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-srx", "Schema name exclude regex filter (ex. 'dbo')", (string schemaFilter) => { _schemaNameExcludeRegexFilter = schemaFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-tx", "Table name exclude filter (ex. 'Student')", (string tableFilter) => { _tableNameExcludeFilter = tableFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-trx", "Table name exclude regex filter (ex. 'Student')", (string tableFilter) => { _tableNameExcludeRegexFilter = tableFilter; }), false);

            _arguments.AddParameter(new InputArgument("-ic", "Don't consider constraints on inserting. Generated columns and so on...", () => _ignoreCostraints = true), false);
            _arguments.AddParameter(new InputArgumentWithInput("-rc", $"Rows count per INSERT. Default: {_rowsPerBatch}", (string count) => _rowsPerBatch = int.Parse(count)), false);
            _arguments.AddParameter(new InputArgument("-v", "Verbose.", () => _verbose = true), false);
            _arguments.AddParameter(new InputArgument("-debug", "Debug.", () => _debug = true), false);

            _arguments.ProcessArguments(args);

            ConsoleWriteLine($"From {_fromdbms}: {_fromConnectionString}. To {_todbms}: {_toConnectionString}. Rows per INSERT: {_rowsPerBatch}. Ignore constraints: {_ignoreCostraints}.");

            if (_debug)
            {
                ConsoleWriteLine("Wait for debugging...Press any key to continue.");
                Console.Read();
            }

            IEnumerable<IModelMetadata> metas = null;

            if (!string.IsNullOrEmpty(_assemblyFileName))
            {
                metas = ProcessAssemblyFile();
                if (_verbose)
                    ConsoleWriteLine($"Assembly analyzing finished.");
            }
            else if (!string.IsNullOrEmpty(_metamodelsFileName))
            {
                metas = ProcessMetamodelsFile();
                if (_verbose)
                    ConsoleWriteLine($"Metamodels file analyzing finished.");
            }

            var opts = new DataMigrationOptions()
            {
                FromConnectionString = _fromConnectionString,
                ToConnectionString = _toConnectionString,
                FromDBMS = _fromdbms,
                ToDBMS = _todbms,
                IgnoreConstraints = _ignoreCostraints,
                RowsPerBatch = _rowsPerBatch,
                Mode = _mode,
                SchemaExcludeNameFilter = _schemaNameExcludeFilter,
                SchemaIncludeNameFilter = _schemaNameIncludeFilter,
                TableExcludeNameFilter = _tableNameExcludeFilter,
                TableIncludeNameFilter = _tableNameIncludeFilter,
                TableIncludeNameRegexFilter = _tableNameIncludeRegexFilter,
                TableExcludeNameRegexFilter = _tableNameExcludeRegexFilter,
                SchemaExcludeNameRegexFilter = _schemaNameExcludeRegexFilter,
                SchemaIncludeNameRegexFilter = _schemaNameIncludeRegexFilter,
                Metadatas = metas
            };

            _worker = new DataMigrationWorker(opts);
            if (_verbose)
                foreach (var p in _worker.RunProgress())
                {
                    if (p.Progress == DataMigrationWorker.E_MIGRATION_PROGRESS.MIGRATING)
                        ConsoleWriteLine($"{p.Metadata.FullObjectName} {p.Progress} {p.InsertedRows}/{p.TotalRows}");
                    else
                        ConsoleWriteLine($"{p.Metadata.FullObjectName} {p.Progress}");
                    GC.Collect(2, GCCollectionMode.Aggressive, true, true);
                }
            else
                _worker.Run();
        }

        private static IEnumerable<IModelMetadata> ProcessAssemblyFile()
        {
            if (!File.Exists(_assemblyFileName))
            {
                ConsoleWriteLine($"Assembly file does not exist: {_assemblyFileName}");
                _arguments.ShowHelp(1);
            }

            _assembly = Assembly.Load(_assemblyFileName);

            List<IModelMetadata> metas = new List<IModelMetadata>();

            foreach (var t in _assembly.DefinedTypes)
            {
                var objectName = t.GetCustomAttribute<DataTools.Attributes.ObjectNameAttribute>();
                if (objectName == null) continue;
                var metadata = typeof(ModelMetadata<>).MakeGenericType(t).GetProperty("Instance").GetValue(null) as IModelMetadata;
                metas.Add(metadata as ModelMetadata);
            }
            return metas;
        }

        private static IEnumerable<IModelMetadata> ProcessMetamodelsFile()
        {
            if (!File.Exists(_metamodelsFileName))
            {
                ConsoleWriteLine($"Metamodels file does not exist: {_metamodelsFileName}");
                _arguments.ShowHelp(1);
            }

            string text = File.ReadAllText(_metamodelsFileName);

            ModelMetadataJSON[] metajs = null;

            var opts = new JsonSerializerOptions();
            opts.Converters.Add(new ModelMetadataJsonConverter());
            opts.Converters.Add(new ModelFieldMetadataJsonConverter());

            try
            {
                metajs = JsonSerializer.Deserialize<ModelMetadataJSON[]>(text, opts);
            }
            catch (Exception e)
            {
                ConsoleWriteLine($"Parsing error! {e.Message} {e.StackTrace}");
                try
                {
                    metajs = new ModelMetadataJSON[] { JsonSerializer.Deserialize<ModelMetadataJSON>(text, opts) };
                }
                catch (Exception e1)
                {
                    ConsoleWriteLine($"Parsing error! {e1.Message} {e.StackTrace}");
                    Environment.Exit(1);
                }
            }

            List<IModelMetadata> metas = new List<IModelMetadata>();
            // сначала загрузить только общее описание моделей для кеширования их имен для будущего связывания по внешним ключах
            foreach (var metaj in metajs)
            {
                var meta = new ModelMetadata();
                meta.ObjectName = metaj.ObjectName;
                meta.SchemaName = metaj.SchemaName;
                meta.IsView = metaj.IsView;
                meta.DisplayModelName = metaj.DisplayModelName;
                meta.ModelTypeName = metaj.ModelTypeName;
                meta.NoUniqueKey = metaj.NoUniqueKey;
                metas.Add(meta);
            }

            // потом загрузить описания колонок
            for (int i = 0; i < metas.Count; ++i)
            {
                var meta = metas[i];
                var metaj = metajs[i];
                foreach (var metafj in metaj.Fields)
                {
                    var mfm = new ModelFieldMetadata();
                    mfm.FieldName = metafj.FieldName;
                    mfm.ColumnName = metafj.ColumnName;
                    mfm.ColumnNames = metafj.ColumnNames.Clone() as string[];
                    mfm.ColumnDisplayName = metafj.ColumnDisplayName;
                    mfm.ColumnDBType = metafj.ColumnDBType;
                    mfm.FieldTypeName = metafj.FieldTypeName;
                    mfm.TextLength = metafj.TextLength;
                    mfm.NumericPrecision = metafj.NumericPrecision;
                    mfm.NumericScale = metafj.NumericScale;
                    mfm.FieldOrder = metafj.FieldOrder;
                    mfm.ForeignColumnNames = metafj.ForeignColumnNames.Clone() as string[];
                    mfm.ForeignModel = string.IsNullOrEmpty(metafj.ForeignModelTypeName) ? null : metas.Where(m => m.FullObjectName == metafj.ForeignModelTypeName).First();
                    mfm.IgnoreChanges = metafj.IgnoreChanges;
                    mfm.IsAutoincrement = metafj.IsAutoincrement;
                    mfm.IsForeignKey = metafj.IsForeignKey;
                    mfm.IsPrimaryKey = metafj.IsPrimaryKey;
                    mfm.IsUnique = metafj.IsUnique;
                    meta.AddField(mfm);
                }
            }

            return metas;
        }
    }
}
