using DataTools.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Deploy
{
    public class MetadataHelper
    {
        /// <summary>
        /// Упорядочить таблицы так, чтобы при их удалении из БД не возникали ошибки из-за внешних связей.
        /// </summary>
        /// <param name="models">Метамодели таблиц, которые могут быть связаны друг с другом.</param>
        /// <returns></returns>
        public static IEnumerable<IModelMetadata> SortForUndeploy(IEnumerable<IModelMetadata> models)
        {
            var alreadyDropped = new List<IModelMetadata>();
            var forDrop = new List<IModelMetadata>(models);
            while (forDrop.Count > 0)
            {
                var droppingMetas = forDrop.Where(def => !isForeign(def, forDrop)).ToArray();
                foreach (var meta in droppingMetas)
                {
                    alreadyDropped.Add(meta);
                    forDrop.Remove(meta);
                }
            }
            return alreadyDropped;
        }

        private static bool isForeign(IModelMetadata foreignModelMetadata, IEnumerable<IModelMetadata> modelMetadatas)
        {
            foreach (var metadata in modelMetadatas)
                if (metadata.Fields.Any(f => f.IsForeignKey && f.ForeignModel.FullObjectName == foreignModelMetadata.FullObjectName && metadata.FullObjectName != foreignModelMetadata.FullObjectName))
                    return true;
            return false;
        }

        /// <summary>
        /// Упорядочить таблицы так, чтобы при их создании в БД не возникали ошибки из-за внешних связей.
        /// </summary>
        /// <param name="models">Метамодели таблиц, которые могут быть связаны друг с другом.</param>
        /// <returns></returns>
        public static IEnumerable<IModelMetadata> SortForDeploy(IEnumerable<IModelMetadata> models)
        {
            var alreadyCreated = new List<IModelMetadata>();
            foreach (var model in models) CreateRecursively(model, alreadyCreated);

            return alreadyCreated;
        }

        private static void CreateRecursively(IModelMetadata modelMetadata, List<IModelMetadata> alreadyCreated)
        {
            if (alreadyCreated.Exists(mm => mm.FullObjectName == modelMetadata.FullObjectName))
                return;
            else
                foreach (var f in modelMetadata.Fields.Where(f => f.IsForeignKey))
                    if (f.ForeignModel.FullObjectName != modelMetadata.FullObjectName)
                        CreateRecursively(f.ForeignModel, alreadyCreated);
            alreadyCreated.Add(modelMetadata);
        }
    }
}
