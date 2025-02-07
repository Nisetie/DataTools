using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Common
{
    public abstract class DataContext : IDataContext
    {
        private ConcurrentStack<IDataSource> _dataSources = new ConcurrentStack<IDataSource>();

        private Dictionary<Type, ICustomModelMapper> _customMappers = new Dictionary<Type, ICustomModelMapper>();
        public void AddCustomMapper<ModelT>(System.Func<IDataContext, object[], ModelT> mapper) where ModelT : class, new()
        {
            _customMappers[typeof(ModelT)] = new CustomModelMapper<ModelT>(mapper);
        }
        private ICustomModelMapper GetCustomMapper<ModelT>() => _customMappers.TryGetValue(typeof(ModelT), out var value) ? value : null;
        public void RemoveCustomMapper<ModelT>() => _customMappers.Remove(typeof(ModelT));

        protected abstract IDataSource _GetDataSource();

        public IDataSource GetDataSource()
        {
            if (_dataSources.TryPop(out var result))
                return result;
            else return _GetDataSource();
        }

        private void _ReturnDataSourceToPool(IDataSource ds)
        {
            _dataSources.Push(ds);
        }

        public virtual IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            var result = _MapResults<ModelT>(ds.ExecuteWithResult(query ?? new SqlSelect().From<ModelT>()));
            this._ReturnDataSourceToPool(ds);
            return result;
        }

        private IEnumerable<ModelT> _MapResults<ModelT>(IEnumerable<object[]> data) where ModelT : class, new()
        {
            var customMapper = GetCustomMapper<ModelT>() as CustomModelMapper<ModelT>;
            if (customMapper == null)
                return ModelMapper<ModelT>.MapModels(this, data);
            else
            {
                var resultList = new List<ModelT>();
                foreach (var dataRow in data)
                    resultList.Add(customMapper.MapModel(this, dataRow));
                return resultList;
            }
        }

        public virtual void Insert<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();

            var insertBuilder = new SqlInsert().Into<ModelT>();
            ModelMapper<ModelT>.BindInsertValues(insertBuilder, model);

            var result = ds.ExecuteWithResult(insertBuilder).ToArray()[0];

            var map = ModelMapper<ModelT>.MapModel;
            var customMapper = GetCustomMapper<ModelT>() as CustomModelMapper<ModelT>;
            if (customMapper != null)
                map = customMapper.MapModel;

            model = map(this, result);

            this._ReturnDataSourceToPool(ds);
        }

        public virtual void Update<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();

            var updateBuilder = new SqlUpdate().From<ModelT>();
            ModelMapper<ModelT>.BindUpdateValues(updateBuilder, model);

            var result = ds.ExecuteWithResult(updateBuilder).ToArray()[0];

            var map = ModelMapper<ModelT>.MapModel;
            var customMapper = GetCustomMapper<ModelT>() as CustomModelMapper<ModelT>;
            if (customMapper != null)
                map = customMapper.MapModel;

            model = map(this, result);
            this._ReturnDataSourceToPool(ds);
        }

        public virtual void Delete<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            var deleteBuilder = new SqlDelete().From<ModelT>();
            ModelMapper<ModelT>.BindDeleteValues(deleteBuilder, model);
            ds.Execute(deleteBuilder);
            this._ReturnDataSourceToPool(ds);
        }

        public void Execute(SqlExpression query)
        {
            var ds = this.GetDataSource();
            ds.Execute(query);
            this._ReturnDataSourceToPool(ds);
        }

        public object ExecuteScalar(SqlExpression query)
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteScalar(query);
            this._ReturnDataSourceToPool(ds);
            return result;
        }

        public IEnumerable<object[]> ExecuteWithResult(SqlExpression query)
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteWithResult(query);
            this._ReturnDataSourceToPool(ds);
            return result;
        }

        public IEnumerable<ModelT> CallTableFunction<ModelT>(SqlFunction function) where ModelT : class, new()
        {
            return Select<ModelT>(new SqlSelect().From<ModelT>(function, "f"));
        }

        public object CallScalarFunction(SqlFunction function)
        {
            return ExecuteScalar(new SqlSelect().Select(function));
        }

        public IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteWithResult(procedure);
            this._ReturnDataSourceToPool(ds);
            return _MapResults<ModelT>(result);
        }

        public void CallProcedure(SqlProcedure procedure)
        {
            var ds = this.GetDataSource();
            ds.Execute(procedure);
            this._ReturnDataSourceToPool(ds);
        }
    }
}