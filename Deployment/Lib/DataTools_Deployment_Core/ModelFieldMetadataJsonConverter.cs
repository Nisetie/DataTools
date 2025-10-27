using DataTools.Common;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataTools.Deploy
{
    public class ModelFieldMetadataJSON : IModelFieldMetadata
    {
        public int FieldOrder { get; set; }
        public string FieldTypeName { get; set; }
        public string FieldName { get; set; }
        public string ColumnName { get; set; }
        public string[] ColumnNames { get; set; }
        public DBType ColumnDBType { get; set; } = null;
        public string ColumnDisplayName { get; set; }
        public bool IsUnique { get; set; }
        public string UniqueConstraintName { get; set; }
        public bool IgnoreChanges { get; set; }
        public bool IsForeignKey { get; set; }
        public string ForeignModelTypeName { get; set; }
        public string[] ForeignColumnNames { get; set; }
        public bool IsAutoincrement { get; set; }
        public bool IsPrimaryKey { get; set; }
        public int? TextLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public IModelMetadata ForeignModel { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool IsPresentation { get; set; }
        public bool IsNullable { get; set; }

        public ModelFieldMetadataJSON() { }

        public ModelFieldMetadataJSON(ModelFieldMetadata modelFieldMetadata)
        {
            this.FieldTypeName = modelFieldMetadata.FieldTypeName;
            this.FieldName = modelFieldMetadata.FieldName;
            this.ColumnName = modelFieldMetadata.ColumnName;
            this.ColumnNames = modelFieldMetadata.ColumnNames?.Clone() as string[] ?? new string[] { };
            this.ColumnDBType = modelFieldMetadata.ColumnDBType;
            this.ColumnDisplayName = modelFieldMetadata.ColumnDisplayName;
            this.IsUnique = modelFieldMetadata.IsUnique;
            this.UniqueConstraintName = modelFieldMetadata.UniqueConstraintName;
            this.IgnoreChanges = modelFieldMetadata.IgnoreChanges;
            this.IsForeignKey = modelFieldMetadata.IsForeignKey;
            this.ForeignModelTypeName = modelFieldMetadata.ForeignModel?.FullObjectName;
            this.ForeignColumnNames = modelFieldMetadata.ForeignColumnNames?.Clone() as string[] ?? new string[] { };
            this.FieldOrder = modelFieldMetadata.FieldOrder;
            this.IsAutoincrement = modelFieldMetadata.IsAutoincrement;
            this.IsPrimaryKey = modelFieldMetadata.IsPrimaryKey;
            this.TextLength = modelFieldMetadata.TextLength;
            this.NumericPrecision = modelFieldMetadata.NumericPrecision;
            this.NumericScale = modelFieldMetadata.NumericScale;
            this.IsPresentation = modelFieldMetadata.IsPresentation;
            this.IsNullable = modelFieldMetadata.IsNullable;
        }

        public IModelFieldMetadata Copy()
        {
            throw new NotImplementedException();
        }
    }

    public class ModelFieldMetadataJsonConverter : JsonConverter<ModelFieldMetadataJSON>
    {
        private ModelFieldMetadataJSON _mfm;

        public override ModelFieldMetadataJSON Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            _mfm = new ModelFieldMetadataJSON();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    var propertyName = reader.GetString();
                    reader.Read();

                    try
                    {
                        switch (propertyName)
                        {
                            case nameof(ModelFieldMetadataJSON.ColumnDisplayName): _mfm.ColumnDisplayName = reader.GetString(); break;
                            case nameof(ModelFieldMetadataJSON.ColumnName): _mfm.ColumnName = reader.GetString(); break;
                            case nameof(ModelFieldMetadataJSON.ColumnNames): _mfm.ColumnNames = reader.GetString().Split(','); break;
                            case nameof(ModelFieldMetadataJSON.ColumnDBType): _mfm.ColumnDBType = DBType.GetDBTypeByName(reader.GetString()); break;
                            case nameof(ModelFieldMetadataJSON.FieldName): _mfm.FieldName = reader.GetString(); break;
                            case nameof(ModelFieldMetadataJSON.FieldTypeName): _mfm.FieldTypeName = reader.GetString(); break;
                            case nameof(ModelFieldMetadataJSON.ForeignColumnNames): _mfm.ForeignColumnNames = reader.GetString().Split(','); break;
                            case nameof(ModelFieldMetadataJSON.ForeignModelTypeName): _mfm.ForeignModelTypeName = reader.GetString(); break;
                            case nameof(ModelFieldMetadataJSON.FieldOrder): _mfm.FieldOrder = reader.GetInt32(); break;
                            case nameof(ModelFieldMetadataJSON.TextLength): _mfm.TextLength = reader.TryGetInt32(out int textLength) ? textLength : (int?)null; break;
                            case nameof(ModelFieldMetadataJSON.IgnoreChanges): _mfm.IgnoreChanges = reader.GetBoolean(); break;
                            case nameof(ModelFieldMetadataJSON.IsUnique): _mfm.IsUnique = reader.GetBoolean(); break;
                            case nameof(ModelFieldMetadataJSON.UniqueConstraintName): _mfm.UniqueConstraintName = reader.GetString(); break;
                            case nameof(ModelFieldMetadataJSON.IsAutoincrement): _mfm.IsAutoincrement = reader.GetBoolean(); break;
                            case nameof(ModelFieldMetadataJSON.IsForeignKey): _mfm.IsForeignKey = reader.GetBoolean(); break;
                            case nameof(ModelFieldMetadataJSON.IsPrimaryKey): _mfm.IsPrimaryKey = reader.GetBoolean(); break;
                            case nameof(ModelFieldMetadataJSON.NumericPrecision): _mfm.NumericPrecision = reader.TryGetInt32(out int numericPrecision) ? numericPrecision : (int?)null; break;
                            case nameof(ModelFieldMetadataJSON.NumericScale): _mfm.NumericScale = reader.TryGetInt32(out int numericScale) ? numericScale : (int?)null; break;
                            case nameof(ModelFieldMetadataJSON.IsPresentation): _mfm.IsPresentation = reader.GetBoolean(); break;
                            case nameof(ModelFieldMetadataJSON.IsNullable): _mfm.IsNullable = reader.GetBoolean(); break;

                            default: reader.Skip(); break;
                        }
                    }
                    catch (Exception)
                    {

                    }
                }
                else if (reader.TokenType == JsonTokenType.EndObject) break;
            }
            return _mfm;
        }

        public override void Write(Utf8JsonWriter writer, ModelFieldMetadataJSON value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString(nameof(ModelFieldMetadataJSON.ColumnDisplayName), value.ColumnDisplayName);
            writer.WriteString(nameof(ModelFieldMetadataJSON.ColumnName), value.ColumnName);
            writer.WriteString(nameof(ModelFieldMetadataJSON.ColumnNames), value.ColumnNames != null ? string.Join(",", value.ColumnNames) : string.Empty);
            writer.WriteString(nameof(ModelFieldMetadataJSON.ColumnDBType), value.ColumnDBType?.ToString() ?? string.Empty);
            writer.WriteString(nameof(ModelFieldMetadataJSON.FieldName), value.FieldName);
            writer.WriteString(nameof(ModelFieldMetadataJSON.FieldTypeName), value.FieldTypeName);
            writer.WriteString(nameof(ModelFieldMetadataJSON.ForeignColumnNames), value.ForeignColumnNames != null ? string.Join(",", value.ForeignColumnNames) : string.Empty);
            writer.WriteString(nameof(ModelFieldMetadataJSON.ForeignModelTypeName), value.ForeignModelTypeName);
            writer.WriteNumber(nameof(ModelFieldMetadataJSON.FieldOrder), value.FieldOrder);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IgnoreChanges), value.IgnoreChanges);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IsAutoincrement), value.IsAutoincrement);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IsForeignKey), value.IsForeignKey);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IsPrimaryKey), value.IsPrimaryKey);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IsUnique), value.IsUnique);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IsPresentation), value.IsPresentation);
            writer.WriteBoolean(nameof(ModelFieldMetadataJSON.IsNullable), value.IsNullable);
            writer.WriteString(nameof(ModelFieldMetadataJSON.UniqueConstraintName), value.UniqueConstraintName);
            if (value.TextLength.HasValue)
                writer.WriteNumber(nameof(ModelFieldMetadataJSON.TextLength), value.TextLength.Value);
            else
                writer.WriteNull(nameof(ModelFieldMetadataJSON.TextLength));
            if (value.NumericPrecision.HasValue)
                writer.WriteNumber(nameof(ModelFieldMetadataJSON.NumericPrecision), value.NumericPrecision.Value);
            else
                writer.WriteNull(nameof(ModelFieldMetadataJSON.NumericPrecision));
            if (value.NumericScale.HasValue)
                writer.WriteNumber(nameof(ModelFieldMetadataJSON.NumericScale), value.NumericScale.Value);
            else
                writer.WriteNull(nameof(ModelFieldMetadataJSON.NumericScale));
            writer.WriteEndObject();
        }
    }

}
