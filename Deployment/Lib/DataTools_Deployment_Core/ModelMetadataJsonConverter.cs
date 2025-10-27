using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataTools.Deploy
{

    public class ModelMetadataJSON : IModelMetadata
    {
        public List<ModelFieldMetadataJSON> Fields { get; set; } = new List<ModelFieldMetadataJSON>();

        public string ModelName { get; set; }
        public string ModelTypeName { get; set; }
        public string SchemaName { get; set; }
        public string ObjectName { get; set; }
        public string DisplayModelName { get; set; }
        public bool NoUniqueKey { get; set; }
        public bool IsView { get; set; }

        public string FullObjectName => throw new NotImplementedException();

        IEnumerable<IModelFieldMetadata> IModelMetadata.Fields => Fields;

        public int FieldsCount => throw new NotImplementedException();

        public ModelMetadataJSON() { }

        public ModelMetadataJSON(ModelMetadata modelMetadata)
        {
            this.ModelName = modelMetadata.ModelName;
            this.ModelTypeName = modelMetadata.ModelTypeName;
            this.SchemaName = modelMetadata.SchemaName;
            this.ObjectName = modelMetadata.ObjectName;
            this.DisplayModelName = modelMetadata.DisplayModelName;
            this.NoUniqueKey = modelMetadata.NoUniqueKey;

            foreach (var field in modelMetadata.Fields)
            {
                Fields.Add(new ModelFieldMetadataJSON(field as ModelFieldMetadata));
            }
        }

        public void AddField(IModelFieldMetadata field)
        {
            throw new NotImplementedException();
        }

        public void RemoveField(IModelFieldMetadata modelFieldMetadata)
        {
            throw new NotImplementedException();
        }

        public IModelFieldMetadata GetColumn(string columnName)
        {
            throw new NotImplementedException();
        }

        public IModelFieldMetadata GetField(string fieldName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IModelFieldMetadata> GetColumnsForSelect()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IModelFieldMetadata> GetColumnsForInsertUpdate()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IModelFieldMetadata> GetColumnsForFilterOrder()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IModelFieldMetadata> GetChangeableFields()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IModelFieldMetadata> GetFilterableFields()
        {
            throw new NotImplementedException();
        }

        public IModelMetadata Copy()
        {
            throw new NotImplementedException();
        }
    }

    public class ModelMetadataJsonConverter : JsonConverter<ModelMetadataJSON>
    {
        private ModelMetadataJSON _currentMetamodel;

        public override ModelMetadataJSON Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            _currentMetamodel = new ModelMetadataJSON();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    var propertyName = reader.GetString();
                    reader.Read();

                    switch (propertyName)
                    {
                        case nameof(ModelMetadata.ObjectName): _currentMetamodel.ObjectName = reader.GetString(); break;
                        case nameof(ModelMetadata.SchemaName): _currentMetamodel.SchemaName = reader.GetString(); break;
                        case nameof(ModelMetadata.ModelName): _currentMetamodel.ModelName = reader.GetString(); break;
                        case nameof(ModelMetadata.ModelTypeName): _currentMetamodel.ModelTypeName = reader.GetString(); break;
                        case nameof(ModelMetadata.DisplayModelName): _currentMetamodel.DisplayModelName = reader.GetString(); break;
                        case nameof(ModelMetadata.NoUniqueKey): _currentMetamodel.NoUniqueKey = reader.GetBoolean(); break;
                        case nameof(ModelMetadata.IsView): _currentMetamodel.IsView = reader.GetBoolean(); break;
                        case nameof(ModelMetadata.Fields): ReadFields(ref reader, typeToConvert, options); break;
                        default: reader.Skip(); break;
                    }
                }
                else if (reader.TokenType == JsonTokenType.EndObject) { break; }
            }
            return _currentMetamodel;
        }

        private void ReadFields(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null) return;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                _currentMetamodel.Fields.Add(JsonSerializer.Deserialize<ModelFieldMetadataJSON>(ref reader, options));
            }
        }

        public override void Write(Utf8JsonWriter writer, ModelMetadataJSON value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString(nameof(ModelMetadataJSON.ObjectName), value.ObjectName);
            writer.WriteString(nameof(ModelMetadataJSON.SchemaName), value.SchemaName);
            writer.WriteString(nameof(ModelMetadataJSON.ModelName), value.ModelName);
            writer.WriteString(nameof(ModelMetadataJSON.ModelTypeName), value.ModelTypeName);
            writer.WriteString(nameof(ModelMetadataJSON.DisplayModelName), value.DisplayModelName);
            writer.WriteBoolean(nameof(ModelMetadataJSON.IsView), value.IsView);
            writer.WriteBoolean(nameof(ModelMetadataJSON.NoUniqueKey), value.NoUniqueKey);
            writer.WriteStartArray(nameof(ModelMetadataJSON.Fields));
            foreach (var field in value.Fields)
            {
                JsonSerializer.Serialize(writer, field, options);
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
    }

}
