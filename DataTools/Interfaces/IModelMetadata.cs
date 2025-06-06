using System;
using System.Collections.Generic;

namespace DataTools.Interfaces
{
    public interface IModelMetadata
    {
        /// <summary>
        /// Название класса в C#
        /// </summary>
        string ModelName { get; set; }
        /// <summary>
        /// Название объекта данных в источнике
        /// </summary>
        string ObjectName { get; set; }
        /// <summary>
        /// Название пространства имен в источнике
        /// </summary>
        string SchemaName { get; set; }
        /// <summary>
        /// Полное название объекта в источнике
        /// </summary>
        string FullObjectName { get; }
        IEnumerable<IModelFieldMetadata> Fields { get; }
        /// <summary>
        /// Тип класса модели
        /// </summary>
        Type ModelType { get; set; }
        /// <summary>
        /// Осмысленное название модели
        /// </summary>
        string DisplayModelName { get; set; }
        bool NoUniqueKey { get; set; }

        void AddField(IModelFieldMetadata field);
        IModelFieldMetadata GetColumn(string columnName);
        IModelFieldMetadata GetField(string fieldName);

        int FieldsCount { get; }
    }
}