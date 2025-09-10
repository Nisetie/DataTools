using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{

    /// <summary>
    /// Данное поле является автоинкрементным и его изменения разрешены только на стороне источника данных.
    /// </summary>
    public class AutoincrementAttribute : FieldAttribute
    {
        public AutoincrementAttribute() { }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsAutoincrement = true;
            metadata.IgnoreChanges = true;
        }
    }
}