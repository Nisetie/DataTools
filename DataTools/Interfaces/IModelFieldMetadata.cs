using DataTools.DML;
using System;

namespace DataTools.Interfaces
{
    public interface IModelFieldMetadata
    {
        int FieldOrder { get; set; }
        string FieldName { get; set; }
        string ColumnName { get; set; }
        string ColumnDisplayName { get; set; }
        Type FieldType { get; set; }
        bool IsUnique { get; set; }
        bool IgnoreChanges { get; set; }
        bool IsForeignKey { get; set; }
        IModelMetadata ForeignModel { get; set; }
        string ForeignColumnName { get; set; }
        bool IsSorted { get; set; }
        int SortOrder { get; set; }
        SqlOrderByClause.E_ORDER SortDirection { get; set; }
        Type ColumnType { get; set; }
        bool IsAutoincrement { get; set; }
    }
}

