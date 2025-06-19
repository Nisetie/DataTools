using Tools.InputArguments;

namespace mssqlgen
{
    internal class Program
    {
        private static ArgumentsCollection _arguments = new ArgumentsCollection();

        private static string _namespaceName;
        private static string _folderPath = null;
        private static string _connectionString;

        static void Main(string[] args)
        {
            _arguments.AddParameter(new InputArgumentWithInput("-n", "Namespace and Library name", (string namespaceName) => { _namespaceName = namespaceName; }), true);
            _arguments.AddParameter(new InputArgumentWithInput("-p", "Save path", (string path) => { _folderPath = path; }), false);
            _arguments.AddParameter(new InputArgumentWithInput("-c", "Connection string", (string cs) => { _connectionString = cs; }), true, "-f");
            _arguments.AddParameter(new InputArgumentWithInput("-f", "Filename with connection string", (string filename) =>
            {
                var cs = File.ReadLines(filename).FirstOrDefault();
                if (!string.IsNullOrEmpty(cs))
                {
                    _connectionString = cs;
                }
                else Console.WriteLine($"Error reading {filename}");
            }), true, "-c");

            _arguments.ProcessArguments(args);

            if (_folderPath == null)
                _folderPath = new FileInfo(Environment.ProcessPath).Directory.FullName;

            var t = new MSSQL_Generator(_namespaceName, _connectionString);

            if (Directory.Exists(_namespaceName))
                Directory.Delete(_namespaceName, true);
            Directory.CreateDirectory(_namespaceName);

            foreach (var modelDef in t.GetModelDefinitions())
            {
                File.WriteAllText(Path.Combine(_namespaceName, $"{modelDef.Catalog}.{modelDef.Schema}.{modelDef.Name}.cs"), modelDef.ModelCode);
            }

            CreateProject(_namespaceName);


        }
        static void CreateProject(string projectName)
        {
            string projectFileContent = $@"
<Project Sdk=""Microsoft.NET.Sdk"">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
  </PropertyGroup>
</Project>";

            File.WriteAllText(Path.Combine(_namespaceName, $"{projectName}.csproj"), projectFileContent);
        }
    }
}
