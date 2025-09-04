using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Длина текстовой колонки на стороне источника данных. -1 - без ограничений.
    /// </summary>
    public class TextLengthAttribute : FieldAttribute
    {
        public int Length { get; set; }

        public TextLengthAttribute(int length) { Length = length; }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.TextLength = Length;
        }
    }
}