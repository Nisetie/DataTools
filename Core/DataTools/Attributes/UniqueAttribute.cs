using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Для автогенератора WHERE в командах UPDATE и DELETE.
    /// </summary>
    public class UniqueAttribute : FieldAttribute
    {
        string ConstraintName;

        public UniqueAttribute() { }

        public UniqueAttribute(string constraintName = "") : this() { ConstraintName = constraintName; }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsUnique = true;
            metadata.UniqueConstraintName = ConstraintName;
        }
    }
}