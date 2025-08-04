using DataTools.Attributes;
using DataTools.Common;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace DataTools.Meta
{
    public class ModelFieldMetadata : IModelFieldMetadata
    {
        public string FieldTypeName { get; set; }
        public string FieldName { get; set; }
        public string ColumnName { get; set; }
        public string[] ColumnNames { get; set; }
        public DBType ColumnType { get; set; } = null;
        public int? TextLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public string ColumnDisplayName { get; set; }
        public bool IsUnique { get; set; }
        public string UniqueConstraintName { get; set; }
        public bool IgnoreChanges { get; set; }
        public bool IsForeignKey { get; set; }
        public IModelMetadata ForeignModel { get; set; }
        public string[] ForeignColumnNames { get; set; }
        public int FieldOrder { get; set; }
        public bool IsAutoincrement { get; set; }
        public bool IsPrimaryKey { get; set; }

        public ModelFieldMetadata() { }

        /// <summary>
        /// Сгенерировать метаданные поля на основе рефлексии и атрибутов
        /// </summary>
        /// <param name="propertyInfo"></param>
        public ModelFieldMetadata(PropertyInfo propertyInfo)
        {
            Type fieldType = propertyInfo.PropertyType;
            FieldName = propertyInfo.Name;
            ColumnName = propertyInfo.Name;
            FieldTypeName = fieldType.AssemblyQualifiedName;
            if (Nullable.GetUnderlyingType(fieldType) != null)
                ColumnType = DBType.GetDBTypeByType(Nullable.GetUnderlyingType(fieldType));
            else 
                ColumnType=  DBType.GetDBTypeByType(fieldType);
            TextLength = -1; // для nvarchar(max), text и т.п.
            IEnumerable<FieldAttribute> attrs = propertyInfo.GetCustomAttributes<FieldAttribute>(true);
            foreach (var attr in attrs)
                attr.ProcessMetadata(propertyInfo, this);
        }

        public ModelFieldMetadata(int fieldOrder, PropertyInfo propertyInfo) : this(propertyInfo) => FieldOrder = fieldOrder;

        public IModelFieldMetadata Copy()
        {
            var field = new ModelFieldMetadata();
            field.FieldTypeName = FieldTypeName;
            field.FieldName = FieldName;
            field.ColumnName = ColumnName;
            field.ColumnNames = ColumnNames == null ? null : ColumnNames.Clone() as string[];
            field.ColumnType = ColumnType;
            field.TextLength = TextLength;
            field.NumericPrecision = NumericPrecision;
            field.NumericScale = NumericScale;
            field.ColumnDisplayName = ColumnDisplayName;
            field.IsUnique = IsUnique;
            field.UniqueConstraintName = UniqueConstraintName;
            field.IgnoreChanges = IgnoreChanges;
            field.IsForeignKey = IsForeignKey;
            field.ForeignModel = ForeignModel;
            field.ForeignColumnNames = ForeignColumnNames == null ? null : ForeignColumnNames.Clone() as string[];
            field.FieldOrder = FieldOrder;
            field.IsAutoincrement = IsAutoincrement;
            field.IsPrimaryKey = IsPrimaryKey;

            return field;
        }
    }
}


