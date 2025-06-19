using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    public class PrimaryKeyAttribute : FieldAttribute
    {
        public PrimaryKeyAttribute() { }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsPrimaryKey = true;
        }
    }
}