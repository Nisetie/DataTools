using DataTools.Interfaces;

namespace DataTools.Attributes
{
    /// <summary>
    /// Явно указать уникальное имя модели в рамках приложения.
    /// </summary>
    public class ModelNameAttribute : ModelAttribute
    {
        public string ModelName;
        public ModelNameAttribute(string modelName) => ModelName = modelName;
        public override void ProcessMetadata(IModelMetadata metadata)
        {
            metadata.ModelName = ModelName;
        }
    }
}