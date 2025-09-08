using DataTools.Interfaces;
using System;

namespace DataTools.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public abstract class ModelAttribute : System.Attribute { public virtual void ProcessMetadata(IModelMetadata metadata) { } }

    /// <summary>
    /// Вспомогательный необязательный атрибут. Полезен при поиске классов моделей внешними анализаторами сборок.
    /// </summary>
    public class ThisIsDataModelAttribute : ModelAttribute { }
}