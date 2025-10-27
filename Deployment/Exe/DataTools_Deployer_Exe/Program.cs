using DataTools.Common;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Tools.InputArguments;

namespace DataTools.Deploy
{


    internal class Program
    {
        private static ArgumentsCollection _arguments;

        private static string _connectionString;
        private static string _assemblyFileName;
        private static string _metamodelsFileName;
        private static Assembly _assembly;
        private static E_DBMS _dbms;
        private static E_DEPLOY_MODE _mode;
        private static IDataContext _context = null;
        private static DBMS_QueryParser _parser = null;
        private static bool _verbose = false;
        private static bool _debug = false;
        private static bool _ignoreCostraints = false;

        private static Dictionary<string, E_DBMS> _dbmsKeys;

        private static DeployerWorker _worker;

        static Program()
        {
            _dbmsKeys = new Dictionary<string, E_DBMS>();
            var names = Enum.GetNames<E_DBMS>();
            foreach (var name in names)
            {
                _dbmsKeys[name.ToLower()] = Enum.Parse<E_DBMS>(name);
            }
        }

        private static void Main(string[] args)
        {
            string processCatalog = Path.GetDirectoryName(Environment.ProcessPath);

            _arguments = new ArgumentsCollection();


            _arguments.AddParameter(new InputArgumentWithInput("-a", "Assembly file path.", (string path) => _assemblyFileName = Path.GetFullPath(path, processCatalog)), true, "-m");
            _arguments.AddParameter(new InputArgumentWithInput("-m", "Metamodels file path.", (string path) => _metamodelsFileName = Path.GetFullPath(path, processCatalog)), true, "-a");
            _arguments.AddParameter(new InputArgumentWithInput("-d", $"DBMS product: {string.Join(',', Enum.GetNames<E_DBMS>().Select(n => n.ToLower()))}.", (string dbms) =>
            {
                _dbms = _dbmsKeys[dbms];
            }), true);
            _arguments.AddParameter(new InputArgumentWithInput("-c", "Connection string", (string cs) => { _connectionString = cs; }), true, "-f");
            _arguments.AddParameter(new InputArgumentWithInput("-f", "Filename with connection string", (string filename) =>
            {
                string cs = string.Empty;
                try
                {
                    cs = File.ReadLines(Path.GetFullPath(filename, processCatalog)).FirstOrDefault();
                    if (!string.IsNullOrEmpty(cs))
                    {
                        _connectionString = cs;
                    }
                    else throw new Exception();
                }
                catch (Exception e)
                {
                    ConsoleWriteLine($"Error reading {cs}. {e.Message}");
                    _arguments.ShowHelp(1);
                }
            }), true, "-c");
            _arguments.AddParameter(new InputArgument("-r", "Redeploy", () => { _mode = E_DEPLOY_MODE.REDEPLOY; }), false, "-u", "-M", "-dc");
            _arguments.AddParameter(new InputArgument("-u", "Undeploy", () => { _mode = E_DEPLOY_MODE.UNDEPLOY; }), false, "-r", "-M", "-dc");
            _arguments.AddParameter(new InputArgumentWithInput("-M", $"Mode: {string.Join(',', Enum.GetNames<E_DEPLOY_MODE>().Select(s => s.ToLower()))}", (string mode) =>
            {
                if (mode == E_DEPLOY_MODE.DEPLOY.ToString().ToLower())
                    _mode = E_DEPLOY_MODE.DEPLOY;
                else if (mode == E_DEPLOY_MODE.REDEPLOY.ToString().ToLower())
                    _mode = E_DEPLOY_MODE.REDEPLOY;
                else if (mode == E_DEPLOY_MODE.UNDEPLOY.ToString().ToLower())
                    _mode = E_DEPLOY_MODE.UNDEPLOY;
                else
                {
                    ConsoleWriteLine($"Unknown mode '{mode}'.");
                    _arguments.ShowHelp(1);
                }

            }), false, "-u", "-r", "-dc");
            _arguments.AddParameter(new InputArgument("-ic", "Don't add ALL constraints to CREATE TABLE commands.", () => _ignoreCostraints = true), false);
            _arguments.AddParameter(new InputArgument("-v", "Verbose.", () => _verbose = true), false);
            _arguments.AddParameter(new InputArgument("-debug", "Wait for debugging.", () => _debug = true), false);

            _arguments.ProcessArguments(args);

            if (_debug)
            {
                ConsoleWriteLine("Wait for debugging...Press any key to continue.");
                Console.Read();
            }

            var opts = new DeployerOptions()
            {
                ConnectionString = _connectionString,
                DBMS = _dbms,
                IgnoreAllCostraints = _ignoreCostraints,
                Mode = _mode
            };

            IEnumerable<IModelMetadata> metas = null;

            if (!string.IsNullOrEmpty(_assemblyFileName))
            {
                if (!File.Exists(_assemblyFileName))
                {
                    ConsoleWriteLine($"Assembly file does not exist: {_assemblyFileName}");
                    _arguments.ShowHelp(1);
                }

                metas = ProcessAssembly();

                if (_verbose)
                    ConsoleWriteLine($"Assembly analyzing finished.");
            }
            else if (!string.IsNullOrEmpty(_metamodelsFileName))
            {
                if (!File.Exists(_metamodelsFileName))
                {
                    ConsoleWriteLine($"Metamodels file does not exist: {_metamodelsFileName}");
                    _arguments.ShowHelp(1);
                }

                metas = ProcessMetamodels();
            }

            opts.Metadatas = metas;

            _worker = new DeployerWorker(opts);

            if (_verbose)
                ConsoleWriteLine("Run worker...");

            try
            {
                if (_verbose)
                    foreach (var info in _worker.RunProgress())
                        ConsoleWriteLine($"{info.Metadata.FullObjectName}: {info.Mode}");
                else _worker.Run();
            }
            catch (Exception e)
            {
                var sb = new StringBuilder();
                while (e != null) {
                    sb.AppendLine(e.Message);
                    sb.AppendLine(e.StackTrace);
                    e = e.InnerException;
                }
                ConsoleWriteLine(sb.ToString());
            }
        }

        private static IEnumerable<IModelMetadata> ProcessAssembly()
        {
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

        private static IEnumerable<IModelMetadata> ProcessMetamodels()
        {
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
                foreach (var ModelFieldMetadataJSON in metaj.Fields)
                {
                    var modelFieldMetadata = new ModelFieldMetadata();
                    modelFieldMetadata.FieldName = ModelFieldMetadataJSON.FieldName;
                    modelFieldMetadata.ColumnName = ModelFieldMetadataJSON.ColumnName;
                    modelFieldMetadata.ColumnNames = ModelFieldMetadataJSON.ColumnNames.Clone() as string[];
                    modelFieldMetadata.ColumnDisplayName = ModelFieldMetadataJSON.ColumnDisplayName;
                    modelFieldMetadata.ColumnDBType = ModelFieldMetadataJSON.ColumnDBType;
                    modelFieldMetadata.FieldTypeName = ModelFieldMetadataJSON.FieldTypeName;
                    modelFieldMetadata.TextLength = ModelFieldMetadataJSON.TextLength;
                    modelFieldMetadata.NumericPrecision = ModelFieldMetadataJSON.NumericPrecision;
                    modelFieldMetadata.NumericScale = ModelFieldMetadataJSON.NumericScale;
                    modelFieldMetadata.FieldOrder = ModelFieldMetadataJSON.FieldOrder;
                    modelFieldMetadata.ForeignColumnNames = ModelFieldMetadataJSON.ForeignColumnNames.Clone() as string[];
                    modelFieldMetadata.ForeignModel = string.IsNullOrEmpty(ModelFieldMetadataJSON.ForeignModelTypeName) ? null : metas.Where(m => m.FullObjectName == ModelFieldMetadataJSON.ForeignModelTypeName).First();
                    modelFieldMetadata.IgnoreChanges = ModelFieldMetadataJSON.IgnoreChanges;
                    modelFieldMetadata.IsAutoincrement = ModelFieldMetadataJSON.IsAutoincrement;
                    modelFieldMetadata.IsForeignKey = ModelFieldMetadataJSON.IsForeignKey;
                    modelFieldMetadata.IsPrimaryKey = ModelFieldMetadataJSON.IsPrimaryKey;
                    modelFieldMetadata.IsUnique = ModelFieldMetadataJSON.IsUnique;
                    modelFieldMetadata.UniqueConstraintName = ModelFieldMetadataJSON.UniqueConstraintName;
                    meta.AddField(modelFieldMetadata);
                }
            }

            return metas;
        }

        private static void ConsoleWriteLine(string message)
        {
            Console.WriteLine($"[{DateTime.Now:o}] {message}");
        }
    }
}





