using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Тип данных столбца на стороне БД
    /// </summary>
    public class ColumnTypeAttribute : FieldAttribute
    {
        public string ColumnType;
        public ColumnTypeAttribute(string columnType) => ColumnType = columnType;

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.ColumnType = ColumnType;
        }
    }
}