using DataTools.Interfaces;
using System.Collections.Generic;
using System.Threading;

namespace DataTools.InMemory
{
    public class InMemory_DataContext : DataTools.Common.DataContext
    {
        private Dictionary<string, IModelMetadata> _metadata = new Dictionary<string, IModelMetadata>();
        private Dictionary<string, IList<object[]>> _data = new Dictionary<string, IList<object[]>>();
        private Dictionary<string, AutoResetEvent> _lockers = new Dictionary<string, AutoResetEvent>();
        private object _locker = new object();

        protected override IDataSource _GetDataSource()
        {
            var ds = new InMemory_DataSource();
            ds.Initialize(this);
            return ds;
        }

        public void AddTable(IModelMetadata metadata)
        {
            lock (_locker)
            {
                string objectName = metadata.FullObjectName;
                _metadata[objectName] = metadata;
                _data[objectName] = new List<object[]>();
                _lockers[objectName] = new AutoResetEvent(true);
            }
        }

        public IModelMetadata GetTableMetadata(string tableName) => _metadata[tableName];
        public IList<object[]> GetData(string tableName) => _data[tableName];

        public void LockTable(string tableName) => _lockers[tableName].WaitOne();
        public void UnlockTable(string tableName) => _lockers[tableName].Set();

    }
}
