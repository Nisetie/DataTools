using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Поле (колонка) является первичным ключом на стороне источника данных.
    /// Это поле (колонка) будет использовать для фильтрации при автоматической генерации команд UPDATE, DELETE.
    /// </summary>
    public class PrimaryKeyAttribute : FieldAttribute
    {
        public PrimaryKeyAttribute() { }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsPrimaryKey = true;
        }
    }
}