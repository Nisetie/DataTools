using DataTools.Common;
using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Тип данных столбца на стороне БД
    /// </summary>
    public class ColumnTypeAttribute : FieldAttribute
    {
        public DBType ColumnType;
        public ColumnTypeAttribute(string columnDBType) => ColumnType = DBType.GetDBTypeByName(columnDBType);

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.ColumnType = ColumnType;
        }
    }
}