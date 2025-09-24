using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Reflection;

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
        /// Составной первичный ключ (или иные поля с ограничением уникальности) преобразуется в строку для универсальности.
        /// </summary>
        public Dictionary<string, ModelT> CachedModels = new Dictionary<string, ModelT>();

        public static readonly string ModelName;

        static SelectModelCache()
        {
            ModelName = ModelMetadata<ModelT>.Instance.ModelName;
        }
        public bool TryGetModelByKey(out ModelT model, string key) => CachedModels.TryGetValue(key, out model);
        public void AddModel(string key, ModelT model) => CachedModels[key] = model;
    }
}

