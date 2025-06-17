using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    public class AutoincrementAttribute : FieldAttribute
    {
        public AutoincrementAttribute() { }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsAutoincrement = true;
        }
    }
}