namespace DataTools
{
    public static class ObjectExtensions
    {
        /// <summary>
        /// Приведение к типу. Если <paramref name="value"/> равен Null, тогда возвращается default(<typeparamref name="T"/>).
        /// Для Nullable-типов default будет равен Null.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T Cast<T>(this object value)
        {
            return value == null ? default(T) : (T)value;
        }
    }
}

