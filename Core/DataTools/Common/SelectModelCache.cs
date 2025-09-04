using DataTools.DML;
using DataTools.Meta;
using System;
using System.Collections.Generic;

namespace DataTools.Common
{
    /// <summary>
    /// Промежуточный кеш сущности. Кеш хранит словарь сущностей "ключ-сущность". Где ключом является строковая конкатенация ключевых полей.
    /// Используется только для команды SELECT.
    /// </summary>
    /// <typeparam name="ModelT"></typeparam>
    public class SelectModelCache<ModelT> : SelectModelCacheBase
        where ModelT : class, new()
    {
        /// <summary>
        /// Составной первичный ключ преобразуется в строку для универсальности.
        /// </summary>
        public Dictionary<string, ModelT> CachedModels = new Dictionary<string, ModelT>();

        private static Func<ModelT, string> _getModelKeyValue;

        public static readonly string ModelName;

        static SelectModelCache()
        {
            ModelName = ModelMetadata<ModelT>.Instance.ModelName;
            _getModelKeyValue = ModelMapper<ModelT>.GetModelKeyValue;
        }

        public bool TryGetModelByKeys(out ModelT model, params object[] keys)
        {
            return CachedModels.TryGetValue(MappingHelper.GetModelUniqueString(keys), out model);
        }

        public void AddModel(ModelT model)
        {
            var keyValue = _getModelKeyValue(model);
            CachedModels[keyValue] = model;
        }
    }
}

