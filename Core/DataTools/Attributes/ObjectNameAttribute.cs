using DataTools.Interfaces;

namespace DataTools.Attributes
{
    /// <summary>
    /// Реальное название объекта в БД
    /// </summary>
    public class ObjectNameAttribute : ModelAttribute
    {
        public string SchemaName = null;
        public string ObjectName;
        public ObjectNameAttribute(string objectName) => ObjectName = objectName.Trim();
        public ObjectNameAttribute(string objectName, string objectSchema) : this(objectName) => SchemaName = objectSchema.Trim();

        public override void ProcessMetadata(IModelMetadata metadata)
        {
            metadata.ObjectName = ObjectName;
            metadata.SchemaName = SchemaName;
        }
    }
}