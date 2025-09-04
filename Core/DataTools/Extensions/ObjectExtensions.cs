using System;
using System.Linq.Expressions;
using System.Reflection;

namespace DataTools.Extensions
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
            if (value == null)
                return default(T);
            else if (TypeExtensions<T>.IsConvertible)
                return (T)Convert.ChangeType(value, TypeExtensions<T>.RealType);
            else if (TypeExtensions<T>.IsParsable)
                return (T)TypeExtensions<T>.ParseMethod.Invoke(null, new object[] { value.ToString() });
            else
                return (T)value;
        }
    }

    public static class TypeExtensions<T>
    {
        public readonly static Type Type;
        public readonly static Type UnboxedType;
        public readonly static bool IsNullable;
        public readonly static Type RealType;
        public readonly static bool IsConvertible;
        public readonly static MethodInfo ParseMethod;
        public readonly static bool IsParsable;
        public readonly static MethodInfo ToStringMethod;

        public static readonly Func<object, T> Cast;

        static TypeExtensions()
        {
            Type = typeof(T);
            UnboxedType = System.Nullable.GetUnderlyingType(Type);
            IsNullable = UnboxedType != null;
            RealType = IsNullable ? UnboxedType : Type;
            IsConvertible = RealType.GetInterface(nameof(IConvertible)) != null;
            ParseMethod = RealType.GetMethod("Parse", new Type[] { typeof(string) });
            IsParsable = ParseMethod != null;
            ToStringMethod = typeof(object).GetMethod(nameof(ToString), new Type[] { });

            var param_value = Expression.Parameter(typeof(object), "value");
            var var_result = Expression.Variable(Type, "result");

            var expr = Expression.Block(
                variables: new ParameterExpression[] { var_result }
                , Expression.IfThenElse(Expression.Equal(param_value, Expression.Constant(null))
                , Expression.Assign(var_result, Expression.Default(Type))
                , Expression.Assign(
                    var_result,
                    IsConvertible
                    ? Expression.Convert(Expression.Call(typeof(Convert), "ChangeType", null, param_value, Expression.Constant(RealType)), Type)
                    : IsParsable
                    ? Expression.Convert(Expression.Call(ParseMethod, Expression.Call(param_value, "ToString", null, null)), Type)
                    : Expression.Convert(param_value, Type)))
                , var_result);
            Cast = Expression.Lambda<Func<object, T>>(expr, param_value).Compile();
        }


    }
}

