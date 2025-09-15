using DataTools.DDL;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Extensions
{
    public static class SqlCreateTableExtensions
    {
        /// <summary>
        /// Указать имя создаваемой таблицы (без схемы)
        /// </summary>
        /// <param name="sqlCreateTable"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static SqlCreateTable Table(this SqlCreateTable sqlCreateTable, string tableName)
        {
            return sqlCreateTable.Table(new DML.SqlName(tableName));
        }

        /// <summary>
        /// Сгенерировать запрос CREATE TABLE для метаданных <paramref name="modelMetadata"/>
        /// </summary>
        /// <param name="sqlCreateTable"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static SqlCreateTable Table(this SqlCreateTable sqlCreateTable, IModelMetadata modelMetadata)
        {
            sqlCreateTable.Table(modelMetadata.FullObjectName);

            List<SqlColumnDefinition> columns = new List<SqlColumnDefinition>();
            List<SqlTableConstraint> constraints = new List<SqlTableConstraint>();
            List<string> primaryKeys = new List<string>();
            Dictionary<string, List<string>> uniques = new Dictionary<string, List<string>>();

            foreach (var modelField in modelMetadata.Fields.OrderBy(f => f.FieldOrder))
            {
                SqlColumnDefinition def = null;
                if (modelField.IsForeignKey)
                {
                    var fkConstraint = new SqlTableForeignKey(modelField.ForeignModel.FullObjectName, modelField.ColumnNames, modelField.ForeignColumnNames);
                    constraints.Add(fkConstraint);

                    for (int i = 0; i < modelField.ForeignColumnNames.Length; i++)
                    {
                        string columnName = modelField.ColumnNames[i];
                        string foreignColumnName = modelField.ForeignColumnNames[i];
                        IModelFieldMetadata foreignModelField = modelField.ForeignModel.GetColumn(foreignColumnName);
                        Type foreignModelFieldType = Type.GetType(foreignModelField.FieldTypeName);

                        def = new SqlColumnDefinition().Name(new DML.SqlName(columnName)).Type(foreignModelField.ColumnType);
                        FillForeignColumnDefinition(def, modelField, foreignModelField, foreignModelFieldType);
                        columns.Add(def);
                        if (modelField.IsPrimaryKey) primaryKeys.Add(modelField.ColumnNames[i]);
                        if (modelField.IsUnique)
                        {
                            if (!uniques.TryGetValue(modelField.UniqueConstraintName ?? "", out var list))
                                uniques[modelField.UniqueConstraintName ?? ""] = list = new List<string>();
                            list.Add(modelField.ColumnNames[i]);
                        }
                    }
                }
                else
                {
                    Type fieldType = Type.GetType(modelField.FieldTypeName);
                    def = new SqlColumnDefinition().Name(new DML.SqlName(modelField.ColumnName)).Type(modelField.ColumnType);
                    FillColumnDefinition(def, modelField, fieldType);
                    columns.Add(def);
                    if (modelField.IsPrimaryKey) primaryKeys.Add(modelField.ColumnName);
                    if (modelField.IsUnique)
                    {
                        if (!uniques.TryGetValue(modelField.UniqueConstraintName ?? "", out var list))
                            uniques[modelField.UniqueConstraintName ?? ""] = list = new List<string>();
                        list.Add(modelField.ColumnName);
                    }
                }
            }

            if (primaryKeys.Count > 0)
                constraints.Add(new SqlTablePrimaryKey(primaryKeys.ToArray()));
            foreach (var u in uniques)
                constraints.Add(new SqlTableUnique(u.Value.ToArray()));

            sqlCreateTable.Column(columns.ToArray());
            sqlCreateTable.Constraint(constraints.ToArray());
            return sqlCreateTable;
        }

        private static void FillColumnDefinition(SqlColumnDefinition def, IModelFieldMetadata modelField, Type fType)
        {
            List<SqlColumnConstraint> constraints = new List<SqlColumnConstraint>();

            if (modelField.IsAutoincrement) constraints.Add(new SqlColumnAutoincrement());

            constraints.Add(new SqlColumnNullable(!(modelField.IsAutoincrement || modelField.IsUnique || modelField.IsPrimaryKey)));

            if (modelField.TextLength != null && fType == typeof(string) || fType == typeof(byte[]))
                def.TextLength = modelField.TextLength;

            if (modelField.NumericPrecision != null && modelField.NumericScale != null && fType == typeof(decimal))
            {
                def.NumericPrecision = modelField.NumericPrecision;
                def.NumericScale = modelField.NumericScale;
            }

            def.Constraint(constraints.ToArray());
        }

        /// <summary>
        /// Что-то берется из метаданных целевой колонки, что-то берется из метаданных колонки внешней таблицы
        /// </summary>
        /// <param name="def"></param>
        /// <param name="modelField"></param>
        /// <param name="foreignModelField"></param>
        /// <param name="fType"></param>
        private static void FillForeignColumnDefinition(SqlColumnDefinition def, IModelFieldMetadata modelField, IModelFieldMetadata foreignModelField, Type fType)
        {
            List<SqlColumnConstraint> constraints = new List<SqlColumnConstraint>();

            constraints.Add(new SqlColumnNullable(!(modelField.IsUnique || modelField.IsPrimaryKey)));

            if (foreignModelField.TextLength != null && fType == typeof(string) || fType == typeof(byte[]))
                def.TextLength = foreignModelField.TextLength;

            if (foreignModelField.NumericPrecision != null && foreignModelField.NumericScale != null && fType == typeof(decimal))
            {
                def.NumericPrecision = foreignModelField.NumericPrecision;
                def.NumericScale = foreignModelField.NumericScale;
            }

            def.Constraint(constraints.ToArray());
        }

        public static SqlCreateTable Table<ModelT>(this SqlCreateTable sqlCreateTable) where ModelT : class, new()
        {
            return Table(sqlCreateTable, ModelMetadata<ModelT>.Instance);
        }
    }
}