using DataTools.Attributes;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DataTools.Meta
{
    public class ModelMetadata : IModelMetadata
    {
        private List<IModelFieldMetadata> _fields = new List<IModelFieldMetadata>();
        private Dictionary<string, IModelFieldMetadata> _fieldsIndex = new Dictionary<string, IModelFieldMetadata>();
        private Dictionary<string, IModelFieldMetadata> _columnsIndex = new Dictionary<string, IModelFieldMetadata>();

        public int FieldsCount => _fields.Count;

        public string ModelName { get; set; }
        public string ModelTypeName { get; set; }

        public string SchemaName { get; set; }
        public string ObjectName { get; set; }

        public string FullObjectName { get => string.IsNullOrEmpty(SchemaName) ? ObjectName : $"{SchemaName}.{ObjectName}"; }

        public string DisplayModelName { get; set; }

        public bool NoUniqueKey { get; set; }

        public IEnumerable<IModelFieldMetadata> Fields => _fields;

        public bool IsView { get; set; }

        public ModelMetadata() { }

        public void AddField(IModelFieldMetadata fieldMetadata)
        {
            _fields.Add(fieldMetadata);
            _columnsIndex[fieldMetadata.ColumnName] = fieldMetadata;
            _fieldsIndex[fieldMetadata.FieldName] = fieldMetadata;
        }
        public void RemoveField(IModelFieldMetadata modelFieldMetadata)
        {
            _fields.Remove(modelFieldMetadata);
        }

        public IModelFieldMetadata GetColumn(string columnName)
        {
            if (_columnsIndex.TryGetValue(columnName, out IModelFieldMetadata fieldMetadata)) return fieldMetadata; return null;
        }
        public IModelFieldMetadata GetField(string fieldName)
        {
            if (_fieldsIndex.TryGetValue(fieldName, out IModelFieldMetadata fieldMetadata)) return fieldMetadata; return null;
        }

        public IEnumerable<string> GetColumnsForSelect()
        {
            foreach (var f in _fields)
            {
                if (f.IsForeignKey)
                {
                    if (f.ColumnNames != null)
                        foreach (var col in f.ColumnNames)
                            yield return col;
                    else yield return f.ColumnName;
                }
                else
                    yield return f.ColumnName;
            }
        }

        public IEnumerable<string> GetColumnsForFilterOrder()
        {
            foreach (var f in _fields)
            {
                if (!(f.IsPrimaryKey || f.IsAutoincrement || f.IsUnique)) continue;

                if (f.IsForeignKey)
                {
                    if (f.ColumnNames != null)
                        foreach (var col in f.ColumnNames)
                            yield return col;
                    else yield return f.ColumnName;
                }
                else
                    yield return f.ColumnName;

            }
        }

        public IEnumerable<string> GetColumnsForInsertUpdate()
        {
            foreach (var f in _fields)
            {
                if (f.IgnoreChanges || f.IsAutoincrement) continue;

                if (f.IsForeignKey)
                {
                    if (f.ColumnNames != null)
                        foreach (var col in f.ColumnNames)
                            yield return col;
                    else yield return f.ColumnName;
                }
                else
                    yield return f.ColumnName;
            }
        }

        public IEnumerable<IModelFieldMetadata> GetChangeableFields()
        {
            return from field
                   in Fields
                   where !(field.IgnoreChanges || field.IsAutoincrement)
                   select field;
        }

        public IEnumerable<IModelFieldMetadata> GetFilterableFields()
        {
            return from field
                   in Fields
                   where field.IsPrimaryKey || field.IsAutoincrement || field.IsUnique
                   select field;
        }

        public IModelMetadata Copy()
        {
            var newMeta = new ModelMetadata();

            newMeta.ModelName = ModelName;
            newMeta.ModelTypeName = ModelTypeName;
            newMeta.SchemaName = SchemaName;
            newMeta.ObjectName = ObjectName;
            newMeta.IsView = IsView;
            newMeta.NoUniqueKey = NoUniqueKey;
            newMeta.DisplayModelName = DisplayModelName;
            foreach (var f in _fields)
            {
                newMeta.AddField(f.Copy());
            }
            return newMeta;
        }

        public static IModelMetadata CreateFromType(Type modelType)
        {
            var instance = new ModelMetadata();
            FillFromType(instance, modelType);
            return instance;
        }
        public static void FillFromType(IModelMetadata instance, Type modelType)
        {
            if (modelType.IsGenericType)
            {
                string modelName = $"{modelType.Name}_{string.Join("_",modelType.GenericTypeArguments as IEnumerable<Type>) }";
                instance.ModelName = modelName;
            }
            else
                instance.ModelName = modelType.Name;
            instance.ModelTypeName = modelType.AssemblyQualifiedName;
            instance.ObjectName = modelType.Name;
            instance.DisplayModelName = modelType.Name;
            instance.NoUniqueKey = false;


            var attrs = modelType.GetCustomAttributes<ModelAttribute>(true);
            foreach (var attr in attrs) attr.ProcessMetadata(instance);

            int i = 0;
            var props = modelType.GetProperties();
            foreach (var prop in props)
            {
                var f = new ModelFieldMetadata(i, prop);
                if ((f.ForeignColumnNames?.Length ?? 0) > 1)
                    i += f.ForeignColumnNames.Length;
                else i++;
                instance.AddField(f);
            }

            // проверка метамодели на отсутствие идентификационного поля
            if (instance.NoUniqueKey == false)
                if (!instance.Fields.Any((f) => f.IsUnique || f.IsAutoincrement || f.IsPrimaryKey))
                    throw new Exception($"Анализ {instance.ModelTypeName}... Нет уникальных полей с атрибутами {nameof(UniqueAttribute)}/{nameof(AutoincrementAttribute)}/{nameof(PrimaryKeyAttribute)}! Укажите как минимум одно поле с атрибутом {nameof(UniqueAttribute)}/{nameof(AutoincrementAttribute)}/{nameof(PrimaryKeyAttribute)}.");
        }
    }

    public static class ModelMetadata<ModelT>
        where ModelT : class, new()
    {
        private static IModelMetadata _instance;

        public static IModelMetadata Instance => _instance;

        static ModelMetadata()
        {
            var modelType = typeof(ModelT);
            _instance = new ModelMetadata();
            ModelMetadata.FillFromType(_instance, modelType);             
        }
    }
}

