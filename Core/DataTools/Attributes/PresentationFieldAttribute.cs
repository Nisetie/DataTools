using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Это поле используется для строкового представления dynamic модели.
    /// </summary>
    public class PresentationFieldAttribute : FieldAttribute
    {
        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsPresentation = true;
        }
    }
}