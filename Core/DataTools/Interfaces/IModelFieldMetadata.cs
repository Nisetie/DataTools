using DataTools.Common;

namespace DataTools.Interfaces
{
    public interface IModelFieldMetadata
    {
        int FieldOrder { get; set; }
        string FieldName { get; set; }
        string ColumnName { get; set; }
        /// <summary>
        /// На случаи, когда поле имеет тип сущности с составным внешним ключом.
        /// Для управления свойством используется атрибут <see cref="DataTools.Attributes.ReferenceAttribute"/>.
        /// </summary>
        string[] ColumnNames { get; set; }
        /// <summary>
        /// NULL или тип из атрибута <see cref="ColumnTypeAttribute"/>
        /// </summary>
        DBType ColumnType { get; set; }
        string ColumnDisplayName { get; set; }
        string FieldTypeName { get; set; }
        bool IsUnique { get; set; }
        bool IgnoreChanges { get; set; }
        bool IsForeignKey { get; set; }
        IModelMetadata ForeignModel { get; set; }
        string[] ForeignColumnNames { get; set; }
        bool IsAutoincrement { get; set; }
        bool IsPrimaryKey { get; set; }
        int? TextLength { get; set; }
        int? NumericPrecision { get; set; }
        int? NumericScale { get; set; }
        string UniqueConstraintName { get; set; }
        bool isPresentation { get; set; }

        IModelFieldMetadata Copy();
    }
}

