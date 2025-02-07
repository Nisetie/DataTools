using DataTools.Attributes;
using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace DataTools.Meta
{
    public class ModelFieldMetadata : IModelFieldMetadata
    {
        public Type FieldType { get; set; }
        public Type ColumnType { get; set; }
        public string FieldName { get; set; }
        public string ColumnName { get; set; }
        public string ColumnDisplayName { get; set; }
        public bool IsUnique { get; set; }
        public bool IgnoreChanges { get; set; }
        public bool IsForeignKey { get; set; }
        public IModelMetadata ForeignModel { get; set; }
        public string ForeignColumnName { get; set; }
        public bool IsSorted { get; set; }
        public int SortOrder { get; set; }
        public SqlOrderByClause.E_ORDER SortDirection { get; set; }
        public int FieldIndex { get; set; }
        public ModelFieldMetadata() { }

        /// <summary>
        /// Сгенерировать метаданные поля на основе рефлексии и атрибутов
        /// </summary>
        /// <param name="propertyInfo"></param>
        public ModelFieldMetadata(PropertyInfo propertyInfo)
        {
            Type fieldType = propertyInfo.PropertyType;
            IEnumerable<FieldAttribute> attrs = propertyInfo.GetCustomAttributes<FieldAttribute>(true);
            FieldName = propertyInfo.Name;
            ColumnName = propertyInfo.Name;
            FieldType = fieldType;
            foreach (var attr in attrs)
            {
                attr.ProcessMetadata(propertyInfo, this);
            }
        }

        public ModelFieldMetadata(int fieldIndex, PropertyInfo propertyInfo) : this(propertyInfo) => FieldIndex = fieldIndex;
    }
}


