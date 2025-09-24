using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.Common
{
    /// <summary>
    /// Промежуточное хранилище сущностей, запрашиваемых по внешним ключам.
    /// Используется только для команды SELECT.
    /// </summary>
    public class SelectCache
    {
        private Dictionary<string, SelectModelCacheBase> _caches = new Dictionary<string, SelectModelCacheBase>();

        public SelectModelCache<ModelT> GetModelCache<ModelT>() where ModelT : class, new()
        {
            string modelName = SelectModelCache<ModelT>.ModelName;
            if (!_caches.TryGetValue(modelName, out var cache))
                _caches[modelName] = cache = new SelectModelCache<ModelT>();
            return cache as SelectModelCache<ModelT>;
        }

        public SelectDynamicCache GetModelCache(IModelMetadata metadata)
        {
            string modelName = metadata.ModelName;
            if (!_caches.TryGetValue(modelName, out var cache))
                _caches[modelName] = cache = new SelectDynamicCache();
            return cache as SelectDynamicCache;
        }
    }
}

