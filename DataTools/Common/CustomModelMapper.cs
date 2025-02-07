using DataTools.Interfaces;
using System;

namespace DataTools.Common
{
    public class CustomModelMapper<ModelT> : ICustomModelMapper where ModelT : class, new()
    {
        private Func<IDataContext, object[], ModelT> _map;

        public CustomModelMapper(Func<IDataContext, object[], ModelT> map)
        {
            _map = map;
        }

        public ModelT MapModel(IDataContext dataContext, object[] values)
        {
            return _map.Invoke(dataContext, values);
        }
    }
}

