using System.Collections.Generic;

namespace DataTools.Common
{
    public static class DataManager
    {
        private static Dictionary<string, DataContext> _contexts;
        static DataManager() => _contexts = new Dictionary<string, DataContext>();
        public static DataContext AddContext(string alias, DataContext context) { return _contexts[alias] = context; }
        public static DataContext GetContext(string alias) { return _contexts[alias]; }
    }
}