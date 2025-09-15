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
        private static Func<object[], string> _getModelUniquestring;

        public static readonly string ModelName;

        static SelectModelCache()
        {
            ModelName = ModelMetadata<ModelT>.Instance.ModelName;
            _getModelKeyValue = ModelMapper<ModelT>.GetModelKeyValue;
            _getModelUniquestring = MappingHelper.GetModelUniqueString;
        }

        public bool TryGetModelByKeys(out ModelT model, params object[] keys) => CachedModels.TryGetValue(_getModelUniquestring(keys), out model);

        public void AddModel(ModelT model) => CachedModels[_getModelKeyValue(model)] = model;
    }
}

