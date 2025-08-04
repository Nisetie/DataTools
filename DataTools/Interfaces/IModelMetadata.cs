using System.Collections.Generic;

namespace DataTools.Interfaces
{
    public interface IModelMetadata
    {
        /// <summary>
        /// Уникальное название модели
        /// </summary>
        string ModelName { get; set; }

        /// <summary>
        /// Название класса в C# для рефлексии
        /// </summary>
        string ModelTypeName { get; set; }
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
        ///// <summary>
        ///// Тип класса модели
        ///// </summary>
        //Type ModelType { get; set; }
        /// <summary>
        /// Осмысленное название модели
        /// </summary>
        string DisplayModelName { get; set; }
        bool NoUniqueKey { get; set; }

        void AddField(IModelFieldMetadata field);
        void RemoveField(IModelFieldMetadata modelFieldMetadata);
        IModelFieldMetadata GetColumn(string columnName);
        IModelFieldMetadata GetField(string fieldName);

        /// <summary>
        /// Получить готовое перечисление колонок для запросов SELECT, INSERT, UPDATE.
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> GetColumnsForSelect();
        IEnumerable<string> GetColumnsForInsertUpdate();

        /// <summary>
        /// Получить перечеть имен колонок, по которым имеет смысл сортировать выборку, т.к.
        /// эти колонки имеют сортирующие ограничения: автоинкремент, первичный ключ и т.д.
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> GetColumnsForOrdering();
        IEnumerable<IModelFieldMetadata> GetChangeableFields();

        int FieldsCount { get; }
        bool IsView { get; set; }

        IModelMetadata Copy();
    }

}