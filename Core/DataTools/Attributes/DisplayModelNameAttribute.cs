using DataTools.Interfaces;

namespace DataTools.Attributes
{
    /// <summary>
    /// Название модели данных для UI 
    /// </summary>
    public class DisplayModelNameAttribute : ModelAttribute
    {
        public string DisplayModelName;
        public DisplayModelNameAttribute(string displayName) => DisplayModelName = displayName;

        public override void ProcessMetadata(IModelMetadata metadata)
        {
            metadata.DisplayModelName = DisplayModelName;
        }
    }
}