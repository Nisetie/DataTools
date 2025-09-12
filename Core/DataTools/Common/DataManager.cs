using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.Common
{
    /// <summary>
    /// Общее хранилище контекстов работы с данными.
    /// </summary>
    public static class DataManager
    {
        private static Dictionary<string, IDataContext> _contexts;
        static DataManager() => _contexts = new Dictionary<string, IDataContext>();
        public static IDataContext AddContext(string alias, IDataContext context) { return _contexts[alias] = context; }
        public static IDataContext GetContext(string alias) { return _contexts[alias]; }
    }
}