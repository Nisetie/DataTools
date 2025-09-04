using DataTools.Interfaces;
using System;

namespace DataTools.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public abstract class ModelAttribute : System.Attribute { public virtual void ProcessMetadata(IModelMetadata metadata) { } }
}