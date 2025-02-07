using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Тип данных, получаемый из БД
    /// </summary>
    public class ColumnTypeAttribute : FieldAttribute
    {
        public System.Type ColumnType;
        public ColumnTypeAttribute(System.Type columnType) => ColumnType = columnType;

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.ColumnType = ColumnType;
        }
    }
}