using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Linq;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Внешний ключ или ссылка на связанную сущность.
    /// </summary>
    public class ReferenceAttribute : FieldAttribute
    {
        public string[] ForeignFieldNames;
        /// <summary>
        /// Если внешний ключ составной, тогда надо явно указать связанные колонки в таблице
        /// </summary>
        public string[] ColumnNames;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="foreignFieldName">Название ПОЛЯ во внешней сущности.</param>
        /// <param name="columnName">Реальное название КОЛОНКИ на стороне источника данных.</param>
        public ReferenceAttribute(string foreignFieldName, string columnName) : this(foreignFieldNames: new[] { foreignFieldName }, columnNames: new[] { columnName }) { }

        /// <summary>
        /// Состав и порядок <paramref name="foreignFieldNames"/> и <paramref name="columnNames"/> должны совпадать!
        /// </summary>
        /// <param name="foreignFieldNames">Названия полей класса внешней сущности. По этим полям автоматически будут уточнены имена колонок.</param>
        /// <param name="columnNames">Названия колонок данной сущности в СУБД.</param>
        public ReferenceAttribute(string[] foreignFieldNames, string[] columnNames)
        {
            ForeignFieldNames = foreignFieldNames;
            ColumnNames = columnNames;
            if ((foreignFieldNames?.Length ?? 0) != (columnNames?.Length ?? 0))
                throw new Exception($"{nameof(ReferenceAttribute)}. Разное количество {nameof(foreignFieldNames)} и {nameof(columnNames)}!");
        }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsForeignKey = true;
            metadata.ForeignModel = (IModelMetadata)typeof(ModelMetadata<>).MakeGenericType(propertyInfo.PropertyType).GetProperty("Instance").GetValue(null);
            metadata.ForeignColumnNames = metadata.ForeignModel.Fields.Where(f => ForeignFieldNames.Contains(f.FieldName)).Select(f => f.ColumnName).ToArray();
            metadata.ColumnNames = ColumnNames;
            if (ColumnNames.Length == 1)
                metadata.ColumnName = ColumnNames[0];
        }
    }
}