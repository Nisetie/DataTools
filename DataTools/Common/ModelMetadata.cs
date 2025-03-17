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

        public int FieldsCount => _fields.Count;

        public Type ModelType { get; set; }
        public string ModelName { get; set; }

        public string SchemaName { get; set; }
        public string ObjectName { get; set; }

        public string FullObjectName { get; set; }

        public string DisplayModelName { get; set; }

        public bool NoUniqueKey { get; set; }

        public IEnumerable<IModelFieldMetadata> Fields => _fields;

        public ModelMetadata() { }

        public void AddField(IModelFieldMetadata fieldMetadata)
        {
            _fields.Add(fieldMetadata);
            _fieldsIndex[fieldMetadata.ColumnName] = fieldMetadata;
        }

        public IModelFieldMetadata GetField(string fieldName)
        {
            //return (from f in _fields where f.FieldName == fieldName select f).FirstOrDefault();
            if (_fieldsIndex.TryGetValue(fieldName, out IModelFieldMetadata fieldMetadata)) return fieldMetadata; return null;
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
            var instance = new ModelMetadata()
            {
                ModelType = modelType,
                ModelName = (modelType.Namespace == null ? "" : modelType.Namespace + ".") + modelType.Name,
                ObjectName = modelType.Name,
                FullObjectName = modelType.Name,
                DisplayModelName = modelType.Name,
                NoUniqueKey = false
            };
            _instance = instance;

            var attrs = modelType.GetCustomAttributes<ModelAttribute>(true);
            foreach (var attr in attrs) attr.ProcessMetadata(_instance);

            int i = 0;
            var props = typeof(ModelT).GetProperties();
            foreach (var prop in props)
            {
                var f = new ModelFieldMetadata(i++, prop);
                instance.AddField(f);
            }

            /// проверка метамодели на отсутствие уникального поля
            if (_instance.NoUniqueKey == false)
                if (!instance.Fields.Any((f) => f.IsUnique))
                    throw new Exception($"Analyzing {instance.ModelName}... No {nameof(UniqueAttribute)} field found! Define unique field in {instance.ModelName}. Use {nameof(UniqueAttribute)}.");

            /// проверка метамоделей на наличие рекурсивных связей
            /// если они есть, тогда эти связи надо разорвать
            /// иначе будет существовать риск бесконечной рекурсии
            var set = new Stack<string>();
            var set1 = new Stack<IModelMetadata>();
            IModelMetadata currentMeta;
            set.Push(instance.ModelName);
            set1.Push(instance);
            while (set1.Count > 0)
            {
                currentMeta = set1.Pop();

                foreach (var f in currentMeta.Fields)
                    if (f.IsForeignKey)
                        if (set.Contains(f.ForeignModel.ModelName))
                            throw new Exception($"Analyzing {instance.ModelName}... Recursive reference found! Remove reference to {f.ForeignModel.ModelName} from {currentMeta.ModelName} model definition.");
                        else
                        {
                            set.Push(f.ForeignModel.ModelName);
                            set1.Push(f.ForeignModel);
                            currentMeta = f.ForeignModel;
                        }
                set.Pop();
            }

        }
    }
}

