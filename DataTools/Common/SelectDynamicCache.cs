using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Common
{
    public class SelectDynamicCache : SelectModelCacheBase
    {
        public Dictionary<string, dynamic> CachedModels = new Dictionary<string, dynamic>();
        public Dictionary<string, SqlParameter> Parameters = new Dictionary<string, SqlParameter>();
        public SqlParameter[] ParametersArray;
        public IModelMetadata Metadata;
        public SqlSelect CachedSelect;
        public DynamicMapper DynamicMapper;

        private Func<dynamic, string> _getModelKeyValue;

        public SelectDynamicCache(IModelMetadata metadata)
        {
            Metadata = metadata;
            DynamicMapper = DynamicMapper.GetMapper(metadata);
            _getModelKeyValue = DynamicMapper.GetModelKeyValue;
            CachedSelect = DynamicMapper.CachedSelect;
            Parameters = DynamicMapper.CachedParameters;
            ParametersArray = Parameters.Values.Select(v => v).ToArray();
        }

        public bool TryGetModelByKeys(out dynamic model, params object[] keys)
        {
            return CachedModels.TryGetValue(MappingHelper.GetModelKey(keys), out model);
        }

        public void AddModel(dynamic model)
        {
            CachedModels[_getModelKeyValue(model)] = model;
        }
    }
}

