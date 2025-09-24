using DataTools.Interfaces;
using System;
using System.Collections.Generic;

namespace DataTools.Common
{
    public class SelectDynamicCache : SelectModelCacheBase
    {
        public Dictionary<string, dynamic> CachedModels = new Dictionary<string, dynamic>();
        public SelectDynamicCache()
        {
        }
        public bool TryGetModelByKey(out dynamic model, in string key) => CachedModels.TryGetValue(key, out model);
        public void AddModel(in string key, dynamic model) => CachedModels[key] = model;
    }
}

