using DataTools.DML;
using DataTools.Interfaces;
using System.Collections.Generic;

namespace DataTools.Meta
{
    public static class MetadataHelper
    {
        public static SqlName[] GetColumnNamesFromColumnMetas(IEnumerable<IModelFieldMetadata> modelFields)
        {
            var columnNamesList = new List<SqlName>();
            foreach (var column in modelFields)
            {
                if (!column.IsForeignKey)
                    columnNamesList.Add(new SqlName(column.ColumnName));
                else
                {
                    if (column.ColumnNames != null && column.ColumnNames.Length > 0)
                        for (int j = 0; j < column.ColumnNames.Length; ++j)
                            columnNamesList.Add(new SqlName(column.ColumnNames[j]));
                    else
                        columnNamesList.Add(new SqlName(column.ColumnName));
                }
            }
            return columnNamesList.ToArray();
        }

        public static SqlOrderByClause[] GetOrderClausesFromColumnMetas(IEnumerable<IModelFieldMetadata> modelFields)
        {
            var columnNamesList = new List<SqlOrderByClause>();
            foreach (var column in modelFields)
            {
                if (!column.IsForeignKey)
                    columnNamesList.Add(new SqlOrderByClause(new SqlName(column.ColumnName)));
                else
                {
                    if (column.ColumnNames != null && column.ColumnNames.Length > 0)
                        for (int j = 0; j < column.ColumnNames.Length; ++j)
                            columnNamesList.Add(new SqlOrderByClause(new SqlName(column.ColumnNames[j])));
                    else
                        columnNamesList.Add(new SqlOrderByClause(new SqlName(column.ColumnName)));
                }
            }
            return columnNamesList.ToArray();
        }
    }
}


