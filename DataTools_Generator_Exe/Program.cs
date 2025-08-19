using DataTools.Common;
using DataTools.Meta;
using System.Text.Json;
using Tools.InputArguments;

namespace DataTools.Deploy
{
    internal class Program
    {
        private static ArgumentsCollection _arguments = new ArgumentsCollection();

        private static string _namespaceName;
        private static string _folderPath = null;
        private static string _connectionString;
        private static bool _forceRecreate = false;
        private static string _schemaNameIncludeFilter = "";
        private static string _tableNameIncludeFilter = "";
        private static string _schemaNameExcludeFilter = "";
        private static string _tableNameExcludeFilter = "";
        private static E_DBMS _dbms;

        private static string _savePath;

        private static bool _verbose = false;

        private static Dictionary<string, E_DBMS> _dbmsKeys;

        private static GeneratorWorker _worker;

        private static bool _debug = false;

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

            _arguments.AddParameter(new InputArgumentWithInput("-n", "Namespace and Library name", (string namespaceName) => { _namespaceName = namespaceName; }), true);
            _arguments.AddParameter(new InputArgumentWithInput("-d", $"DBMS product: {string.Join(',', Enum.GetNames<E_DBMS>().Select(n => n.ToLower()))}.", (string dbms) =>
            {
                _dbms = _dbmsKeys[dbms];
            }), true);
            _arguments.AddParameter(new InputArgumentWithInput("-s", "Schema name include filter (ex. 'dbo', 'test')", (string schemaFilter) => { _schemaNameIncludeFilter = schemaFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-t", "Table name include filter (ex. 'Student', 'Test')", (string tableFilter) => { _tableNameIncludeFilter = tableFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-sx", "Schema name exclude filter (ex. 'dbo', 'test')", (string schemaFilter) => { _schemaNameExcludeFilter = schemaFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-tx", "Table name exclude filter (ex. 'Student', 'Test')", (string tableFilter) => { _tableNameExcludeFilter = tableFilter; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-p", "Save path", (string path) => { _folderPath = Path.GetFullPath(path, processCatalog); }), false);
            _arguments.AddParameter(new InputArgument("-r", "Recreate project folder", () => { _forceRecreate = true; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-c", "Connection string", (string cs) => { _connectionString = cs; }), true, "-f");
            _arguments.AddParameter(new InputArgumentWithInput("-f", "Filename with connection string", (string filename) =>
            {
                var cs = File.ReadLines(Path.GetFullPath(filename, processCatalog)).FirstOrDefault();
                if (!string.IsNullOrEmpty(cs))
                {
                    _connectionString = cs;
                }
                else
                {
                    Console.WriteLine($"Empty {filename}!");
                    Environment.Exit(1);
                }
            }), true, "-c");

            _arguments.AddParameter(new InputArgument("-v", "Verbose.", () => _verbose = true), false);
            _arguments.AddParameter(new InputArgument("-debug", "For debug.", () => _debug = true), false);

            _arguments.ProcessArguments(args);

            if (_debug)
            {
                ConsoleWriteLine("Wait for debugging...Press any key to continue.");
                Console.Read();
            }

            var opts = new GeneratorOptions()
            {
                DBMS = _dbms,
                ConnectionString = _connectionString,
                NamespaceName = _namespaceName,
                SchemaIncludeNameFilter = _schemaNameIncludeFilter,
                TableIncludeNameFilter = _tableNameIncludeFilter,
                SchemaExcludeNameFilter = _schemaNameExcludeFilter,
                TableExcludeNameFilter = _tableNameExcludeFilter
            };

            _worker = new GeneratorWorker(opts);

            if (_folderPath == null)
                _folderPath = processCatalog;

            _savePath = Path.Combine(_folderPath, _namespaceName);

            if (Directory.Exists(_savePath))
            {
                if (_forceRecreate)
                {
                    if (_verbose)
                        ConsoleWriteLine($"Removing folder: {_savePath}");
                    Directory.Delete(_savePath, true);
                }
                else
                {
                    Console.WriteLine($"The folder ({_savePath}) is already exists. Use -r for recreation this folder.");
                }
            }
            if (_verbose)
                ConsoleWriteLine($"Creating folder: {_savePath}");
            Directory.CreateDirectory(_savePath);

            List<ModelMetadataJSON> metas = new List<ModelMetadataJSON>();

            if (_verbose)
                ConsoleWriteLine($"Reading metadata from {_dbms} ({_connectionString})...");
            foreach (var modelDef in _worker.GetModelDefinitions())
            {
                if (_verbose)
                    ConsoleWriteLine($"Processing: {modelDef.Schema} {modelDef.Name}");
                var modelDirectory = Path.Combine(_savePath, modelDef.Schema);
                if (!Directory.Exists(modelDirectory))
                    Directory.CreateDirectory(modelDirectory);
                File.WriteAllText(Path.Combine(modelDirectory, $"{modelDef.Name}.cs"), modelDef.ModelCode);
                //if (modelDef.Name == "BuildVersion") ConsoleWriteLine(JsonSerializer.Serialize(modelDef.ModelMetadata));
                metas.Add(new ModelMetadataJSON(modelDef.ModelMetadata as ModelMetadata));
            }

            var jsonSerializerOptions = new JsonSerializerOptions();
            jsonSerializerOptions.Converters.Add(new ModelMetadataJsonConverter());
            jsonSerializerOptions.Converters.Add(new ModelFieldMetadataJsonConverter());

            var text = JsonSerializer.Serialize(metas.ToArray(), jsonSerializerOptions);
            File.WriteAllText(Path.Combine(_savePath, $"{_namespaceName}_metadata.json"), text);

            CreateProject(_namespaceName);
        }
        private static void CreateProject(string projectName)
        {
            string projectFileContent = $@"
<Project Sdk=""Microsoft.NET.Sdk"">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <ReferenceOutputAssembly>false</ReferenceOutputAssembly>
    <DebugType>None</DebugType>
    <DebugSymbols>False</DebugSymbols>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include=""DataTools""/>
  </ItemGroup>
</Project>";

            File.WriteAllText(Path.Combine(_savePath, $"{projectName}.csproj"), projectFileContent);
        }

        private static void ConsoleWriteLine(string message)
        {
            Console.WriteLine($"[{DateTime.Now:o}] {message}");
        }
    }
}
