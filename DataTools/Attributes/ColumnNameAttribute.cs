using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Реальное название колонки в БД
    /// </summary>
    public class ColumnNameAttribute : FieldAttribute
    {
        public string ColumnName;
        public ColumnNameAttribute(string columnName) => ColumnName = columnName;

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.ColumnName = ColumnName;
        }
    }
}