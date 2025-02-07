using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Для игнорирования поля в INSERT, UPDATE.
    /// Например, для автоинкрементных полей.
    /// </summary>
    public class IgnoreChangesAttribute : FieldAttribute
    {
        public IgnoreChangesAttribute() { }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IgnoreChanges = true;
        }
    }
}