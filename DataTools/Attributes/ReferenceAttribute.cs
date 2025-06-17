using DataTools.Interfaces;
using DataTools.Meta;
using System.Linq;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Внешний ключ или ссылка на связанную сущность
    /// </summary>
    public class ReferenceAttribute : FieldAttribute
    {
        public string ForeignFieldName;

        public ReferenceAttribute(string foreignColumnName)
        {
            ForeignFieldName = foreignColumnName;
        }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsForeignKey = true;
            metadata.ForeignModel = (IModelMetadata)typeof(ModelMetadata<>).MakeGenericType(propertyInfo.PropertyType).GetProperty("Instance").GetValue(null);
            metadata.ForeignColumnName = metadata.ForeignModel.Fields.First(f => f.FieldName == ForeignFieldName).ColumnName;
        }
    }
}