using DataTools.Interfaces;
using System;
using System.Reflection;

namespace DataTools.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public abstract class FieldAttribute : System.Attribute { public virtual void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata) { } }
}