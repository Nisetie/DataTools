using DataTools.Interfaces;
using System.Reflection;

namespace DataTools.Attributes
{
    /// <summary>
    /// Точность вещественного числа: длина, количество цифр после запятой.
    /// </summary>
    public class NumericAttribute : FieldAttribute
    {
        public int NumericPrecision, NumericScale;

        public NumericAttribute(int numericPrecision, int numericScale)
        {
            NumericPrecision = numericPrecision;
            NumericScale = numericScale;
        }
        public override void ProcessMetadata(PropertyInfo propertyInfo, IModelFieldMetadata metadata)
        {
            metadata.NumericPrecision = NumericPrecision;
            metadata.NumericScale = NumericScale;
        }
    }
}