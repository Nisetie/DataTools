using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.InMemory
{
    public class InMemory_DataContext : DataTools.Common.DataContext
    {
        private Dictionary<string, IModelMetadata> _metadata = new Dictionary<string, IModelMetadata>();
        private Dictionary<string, IList<object[]>> _data = new Dictionary<string, IList<object[]>>();

        protected override IDataSource _GetDataSource()
        {
            var ds = new InMemory_DataSource();
            ds.Initialize(this);
            return ds;
        }

        public void AddTable(IModelMetadata metadata)
        {
            _metadata[metadata.FullObjectName] = metadata;
            _data[metadata.FullObjectName] = new List<object[]>();
        }

        public IModelMetadata GetTableMetadata(string tableName) => _metadata[tableName];
        public IList<object[]> GetData(string tableName) => _data[tableName];

    }
}
