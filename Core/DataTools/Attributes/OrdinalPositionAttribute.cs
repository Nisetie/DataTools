using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Явно задать порядок поля-колонки в командах SELECT, UPDATE, INSERT
    /// </summary>
    public class OrdinalPositionAttribute : FieldAttribute
    {
        public int OrdinalPosition;
        public OrdinalPositionAttribute(int pos) => OrdinalPosition = pos;

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.FieldOrder = OrdinalPosition;
        }
    }
}