using DataTools.Common;
using System.Collections.Generic;

namespace DataTools.Deploy
{
    public class GeneratorOptions
    {
        public string NamespaceName { get; set; }
        public string ConnectionString { get; set; }
        public string SchemaIncludeNameFilter { get; set; } = "";
        public string TableIncludeNameFilter { get; set; } = "";
        public string SchemaExcludeNameFilter { get; set; } = "";
        public string TableExcludeNameFilter { get; set; } = "";

        public E_DBMS DBMS { get; set; }
    }

    public class GeneratorWorker : GeneratorOptions
    {
        private GeneratorBase _generator;

        public GeneratorWorker(GeneratorOptions options)
        {
            this.NamespaceName = options.NamespaceName;
            this.ConnectionString = options.ConnectionString;
            this.SchemaIncludeNameFilter = options.SchemaIncludeNameFilter;
            this.TableIncludeNameFilter = options.TableIncludeNameFilter;
            this.TableExcludeNameFilter = options.TableExcludeNameFilter;
            this.SchemaExcludeNameFilter = options.SchemaExcludeNameFilter;
            this.DBMS = options.DBMS;
        }

        public IEnumerable<ModelDefinition> GetModelDefinitions()
        {
            switch (DBMS)
            {
                case E_DBMS.MSSQL: _generator = new MSSQL_Generator(ConnectionString); break;
                case E_DBMS.PostgreSQL: _generator = new PostgreSQL_Generator(ConnectionString); break;
                case E_DBMS.SQLite: _generator = new SQLite_Generator(ConnectionString); break;
            }

            return _generator.GetModelDefinitions(TableIncludeNameFilter, SchemaIncludeNameFilter, TableExcludeNameFilter, SchemaExcludeNameFilter);
        }
    }
}
