using DataTools.DML;
using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Для сортировки
    /// </summary>
    public class SortAttribute : FieldAttribute
    {
        public int SortOrder { get; set; }
        public SqlOrderByClause.E_ORDER SortDirection { get; set; }
        public SortAttribute(int order = int.MaxValue, SqlOrderByClause.E_ORDER orderDirection = SqlOrderByClause.E_ORDER.ASC)
        {
            SortOrder = order;
            SortDirection = orderDirection;
        }

        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.IsSorted = true;
            metadata.SortOrder = SortOrder;
            metadata.SortDirection = SortDirection;
        }
    }
}