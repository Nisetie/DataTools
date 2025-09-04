using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Осмысленное название колонки
    /// </summary>
    public class DisplayFieldNameAttribute : FieldAttribute
    {
        public string ColumnDisplayName;
        public DisplayFieldNameAttribute(string columnDisplayName) => ColumnDisplayName = columnDisplayName;

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.ColumnDisplayName = ColumnDisplayName;
        }
    }
}