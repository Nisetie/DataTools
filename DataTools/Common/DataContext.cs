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

        private Dictionary<Type, Func<IDataContext, object[], object>> _customModelMappers = new Dictionary<Type, Func<IDataContext, object[], object>>();
        private Dictionary<Type, Func<object, object>> _customTypeConverters = new Dictionary<Type, Func<object, object>>();
        public void AddCustomMapper<ModelT>(Func<IDataContext, object[], object> mapper) where ModelT : class, new()
        {
            _customModelMappers[typeof(ModelT)] = mapper;
        }
        public void AddCustomTypeConverter<T>(Func<object, object> converter)
        {
            _customTypeConverters[typeof(T)] = converter;
        }
        private Func<IDataContext, object[], object> GetCustomMapper<ModelT>() => _customModelMappers.TryGetValue(typeof(ModelT), out var value) ? value : null;
        public void RemoveCustomMapper<ModelT>() => _customModelMappers.Remove(typeof(ModelT));

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

        public IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                var rawResult = ds.ExecuteWithResult(query ?? new SqlSelect().From<ModelT>());
                var queryCache = new QueryCache();
                var customMapper = GetCustomMapper<ModelT>();
                if (customMapper == null)
                    foreach (var dataRow in rawResult)
                        yield return ModelMapper<ModelT>.MapModel(this, _customTypeConverters, dataRow, queryCache);
                else
                    foreach (var dataRow in rawResult)
                        yield return (ModelT)customMapper(this, dataRow);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                var rawResult = ds.ExecuteWithResult(query ?? new SqlSelect().From<ModelT>(), parameters);
                var queryCache = new QueryCache();
                var customMapper = GetCustomMapper<ModelT>();
                if (customMapper == null)
                    foreach (var dataRow in rawResult)
                        yield return ModelMapper<ModelT>.MapModel(this, _customTypeConverters, dataRow, queryCache);
                else
                    foreach (var dataRow in rawResult)
                        yield return (ModelT)customMapper(this, dataRow);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Insert<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            var insertBuilder = new SqlInsert().Into<ModelT>();
            ModelMapper<ModelT>.BindInsertValues(insertBuilder, model);
            var result = ds.ExecuteWithResult(insertBuilder).ToArray()[0];
            var queryCache = new QueryCache();
            var map = ModelMapper<ModelT>.MapModel;
            var customMapper = GetCustomMapper<ModelT>();
            if (customMapper != null)
                model = (ModelT)customMapper(this, result);
            else
                model = ModelMapper<ModelT>.MapModel(this, _customTypeConverters, result, queryCache);
            this._ReturnDataSourceToPool(ds);
        }

        public void Update<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();

            var updateBuilder = new SqlUpdate().From<ModelT>();
            ModelMapper<ModelT>.BindUpdateValues(updateBuilder, model);

            var result = ds.ExecuteWithResult(updateBuilder).ToArray()[0];
            var queryCache = new QueryCache();
            var map = ModelMapper<ModelT>.MapModel;
            var customMapper = GetCustomMapper<ModelT>();
            if (customMapper != null)
                model = (ModelT)customMapper(this, result);
            else
                model = ModelMapper<ModelT>.MapModel(this, _customTypeConverters, result, queryCache);
            this._ReturnDataSourceToPool(ds);
        }

        public void Delete<ModelT>(ModelT model) where ModelT : class, new()
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
        public void Execute(SqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            ds.Execute(query, parameters);
            this._ReturnDataSourceToPool(ds);
        }

        public object ExecuteScalar(SqlExpression query)
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteScalar(query);
            this._ReturnDataSourceToPool(ds);
            return result;
        }
        public object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteScalar(query, parameters);
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
        public IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteWithResult(query, parameters);
            this._ReturnDataSourceToPool(ds);
            return result;
        }

        public IEnumerable<ModelT> CallTableFunction<ModelT>(SqlFunction function, params SqlParameter[] parameters) where ModelT : class, new()
        {
            return Select<ModelT>(new SqlSelect().From<ModelT>(function, "f"), parameters);
        }

        public object CallScalarFunction(SqlFunction function, params SqlParameter[] parameters)
        {
            return ExecuteScalar(new SqlSelect().Select(function), parameters);
        }

        public IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            var result = ds.ExecuteWithResult(procedure, parameters);
            var queryCache = new QueryCache();
            var customMapper = GetCustomMapper<ModelT>();
            if (customMapper == null)
                foreach (var dataRow in result)
                    yield return ModelMapper<ModelT>.MapModel(this, _customTypeConverters, dataRow, queryCache);
            else
                foreach (var dataRow in result)
                    yield return (ModelT)customMapper(this, dataRow);

            this._ReturnDataSourceToPool(ds);
        }

        public void CallProcedure(SqlProcedure procedure, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            ds.Execute(procedure, parameters);
            this._ReturnDataSourceToPool(ds);
        }

        public abstract void CreateTable<ModelT>() where ModelT : class, new();

        public abstract void DropTable<ModelT>() where ModelT : class, new();
    }
}