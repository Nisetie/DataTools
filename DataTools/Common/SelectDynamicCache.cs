using DataTools.Interfaces;
using System;
using System.Collections.Generic;

namespace DataTools.Common
{
    public class SelectDynamicCache : SelectModelCacheBase
    {
        public Dictionary<string, dynamic> CachedModels = new Dictionary<string, dynamic>();
        public DynamicMapper DynamicMapper;

        private Func<dynamic, string> _getModelKeyValue;

        public SelectDynamicCache(IModelMetadata metadata)
        {
            DynamicMapper = DynamicMapper.GetMapper(metadata);
            _getModelKeyValue = DynamicMapper.GetModelKeyValue;
        }

        public bool TryGetModelByKeys(out dynamic model, params object[] keys)
        {
            return CachedModels.TryGetValue(MappingHelper.GetModelUniqueString(keys), out model);
        }

        public void AddModel(dynamic model)
        {
            CachedModels[_getModelKeyValue(model)] = model;
        }
    }
}

