using DataTools.Interfaces;

namespace DataTools.Attributes
{
    /// <summary>
    /// К модели не применять проверку на наличие первичного ключа - атрибута Unique.
    /// Использовать атрибут можно в случаях, если у модели нет внешних связей и к ней
    /// не планируется применять команды Update, Delete. Это подойдет, например, к представлениям.
    /// </summary>
    public class NoUnique : ModelAttribute
    {
        public override void ProcessMetadata(IModelMetadata metadata)
        {
            metadata.NoUniqueKey = true;
        }
    }
}