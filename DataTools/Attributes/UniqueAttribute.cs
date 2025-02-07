using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Для автогенератора WHERE в командах UPDATE и DELETE.
    /// </summary>
    public class UniqueAttribute : FieldAttribute
    {
        public UniqueAttribute() { }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsUnique = true;
        }
    }
}