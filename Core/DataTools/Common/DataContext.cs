using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Schema;

namespace DataTools.Common
{
    /// <summary>
    /// Объединяет в себе:
    /// - строку подключения к СУБД;
    /// - произвольные мапперы моделей и базовых типов;
    /// - потокобезопасный пул объектов IDataSource.
    /// </summary>
    public abstract class DataContext : IDataContext
    {
        private ConcurrentStack<IDataSource> _dataSources = new ConcurrentStack<IDataSource>();

        private Dictionary<string, Func<IDataContext, object[], object>> _customModelMappers = new Dictionary<string, Func<IDataContext, object[], object>>();

        private Dictionary<Type, Func<object, object>> _customTypeConverters = new Dictionary<Type, Func<object, object>>();
        public void AddCustomModelMapper<ModelT>(Func<IDataContext, object[], object> mapper) where ModelT : class, new()
        {
            AddCustomModelMapper(mapper, ModelMetadata<ModelT>.Instance);
        }
        public void AddCustomModelMapper(Func<IDataContext, object[], object> mapper, IModelMetadata metadata)
        {
            _customModelMappers[metadata.ModelName] = mapper;
        }
        private Func<IDataContext, object[], object> GetCustomModelMapper<ModelT>() where ModelT : class, new()
        {
            return GetCustomModelMapper(ModelMetadata<ModelT>.Instance);
        }
        private Func<IDataContext, object[], object> GetCustomModelMapper(IModelMetadata metadata)
        {
            return _customModelMappers.TryGetValue(metadata.ModelName, out var value) ? value : null;
        }
        public void RemoveCustomModelMapper<ModelT>() where ModelT : class, new()
        {
            RemoveCustomModelMapper(ModelMetadata<ModelT>.Instance);
        }
        public void RemoveCustomModelMapper(IModelMetadata metadata)
        {
            _customModelMappers.Remove(metadata.ModelName);
        }

        public void AddCustomTypeConverter<T>(Func<object, object> converter)
        {
            _customTypeConverters[typeof(T)] = converter;
        }
        public void RemoveCustomTypeConverter<T>()
        {
            _customTypeConverters.Remove(typeof(T));
        }

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

        public IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                if (query == null) query = new SqlSelect().From<ModelT>();
                foreach (var row in ReturnResultExact<ModelT>(ds, query, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }      

        public IEnumerable<dynamic> Select(IModelMetadata metadata, SqlExpression query = null, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                if (query == null) query = new SqlSelect().From(metadata);
                foreach (var row in ReturnResultDynamic(metadata, ds, query, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Insert<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                var insertBuilder = new SqlInsert().Into<ModelT>();
                ModelMapper<ModelT>.BindInsertValues(insertBuilder, model);
                var result = ds.ExecuteWithResult(insertBuilder).ToArray()[0];
                var queryCache = new SelectCache();
                var customMapper = GetCustomModelMapper(ModelMetadata<ModelT>.Instance);
                if (customMapper != null)
                    model = (ModelT)customMapper(this, result);
                else
                {
                    model = ModelMapper<ModelT>.MapModel(this, _customTypeConverters, result, queryCache);
                }
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }
        public void Insert(IModelMetadata modelMetadata, dynamic model)
        {
            var ds = this.GetDataSource();
            try
            {
                var insertBuilder = new SqlInsert().Into(modelMetadata);
                var queryCache = new SelectCache();
                var mapper = DynamicMapper.GetMapper(modelMetadata);
                mapper.BindInsertValues(insertBuilder, model);
                var result = ds.ExecuteWithResult(insertBuilder).ToArray()[0];
                var customMapper = GetCustomModelMapper(modelMetadata);
                if (customMapper != null)
                    model = customMapper(this, result);
                else
                    model = mapper.MapModel(this, _customTypeConverters, result, queryCache);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Update<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                var updateBuilder = new SqlUpdate().From<ModelT>();
                ModelMapper<ModelT>.BindUpdateValues(updateBuilder, model);
                ModelMapper<ModelT>.BindUpdateWhere(updateBuilder, model);

                var result = ds.ExecuteWithResult(updateBuilder).ToArray()[0];
                var queryCache = new SelectCache();
                var customMapper = GetCustomModelMapper(ModelMetadata<ModelT>.Instance);
                if (customMapper != null)
                    model = (ModelT)customMapper(this, result);
                else
                    model = ModelMapper<ModelT>.MapModel(this, _customTypeConverters, result, queryCache);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Update(IModelMetadata modelMetadata, dynamic model)
        {
            var ds = this.GetDataSource();
            try
            {
                var updateBuilder = new SqlUpdate().From(modelMetadata);
                var queryCache = new SelectCache();
                var mapper = DynamicMapper.GetMapper(modelMetadata);

                mapper.BindUpdateValues(updateBuilder, model);
                mapper.BindUpdateWhere(updateBuilder, model);

                var result = ds.ExecuteWithResult(updateBuilder).ToArray()[0];

                var customMapper = GetCustomModelMapper(modelMetadata);
                if (customMapper != null)
                    model = customMapper(this, result);
                else
                    model = mapper.MapModel(this, _customTypeConverters, result, queryCache);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Delete<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                var deleteBuilder = new SqlDelete().From<ModelT>();
                ModelMapper<ModelT>.BindDeleteWhere(deleteBuilder, model);
                ds.Execute(deleteBuilder);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }
        public void Delete(IModelMetadata modelMetadata, dynamic model)
        {
            var ds = this.GetDataSource();
            try
            {
                var deleteBuilder = new SqlDelete().From(modelMetadata);
                var mapper = DynamicMapper.GetMapper(modelMetadata);
                mapper.BindDeleteWhere(deleteBuilder, model);
                ds.Execute(deleteBuilder);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Execute(SqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                ds.Execute(query, parameters);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                var result = ds.ExecuteScalar(query, parameters);
                return result;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                var result = ds.ExecuteWithResult(query, parameters);
                return result;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<ModelT> CallTableFunction<ModelT>(SqlFunction function, params SqlParameter[] parameters) where ModelT : class, new()
        {
            return Select<ModelT>(new SqlSelect().From<ModelT>(function, "f"), parameters);
        }
        public IEnumerable<dynamic> CallTableFunction(IModelMetadata modelMetadata, SqlFunction function, params SqlParameter[] parameters)
        {
            return Select(modelMetadata, new SqlSelect().From(modelMetadata, function, "f"), parameters);
        }

        public object CallScalarFunction(SqlFunction function, params SqlParameter[] parameters)
        {
            return ExecuteScalar(new SqlSelect().Select(function), parameters);
        }

        public IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                foreach (var row in ReturnResultExact<ModelT>(ds, procedure, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<dynamic> CallProcedure(IModelMetadata modelMetadata, SqlProcedure procedure, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                foreach (var row in ReturnResultDynamic(modelMetadata, ds, procedure, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        private IEnumerable<dynamic> ReturnResultDynamic(IModelMetadata modelMetadata, IDataSource ds, SqlExpression query, params SqlParameter[] parameters)
        {
            var result = ds.ExecuteWithResult(query, parameters);
            var queryCache = new SelectCache();

            var customMapper = GetCustomModelMapper(modelMetadata);
            if (customMapper != null)
                foreach (var dataRow in result)
                    yield return customMapper(this, dataRow);
            else
            {
                var map = DynamicMapper.GetMapper(modelMetadata).MapModel;
                foreach (var dataRow in result)
                    yield return map(this, _customTypeConverters, dataRow, queryCache);
            }
        }

        private IEnumerable<ModelT> ReturnResultExact<ModelT>(IDataSource ds, SqlExpression query, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var result = ds.ExecuteWithResult(query, parameters);
            var queryCache = new SelectCache();
            var customMapper = GetCustomModelMapper(ModelMetadata<ModelT>.Instance);
            if (customMapper != null)
                foreach (var dataRow in result)
                    yield return (ModelT)customMapper(this, dataRow);
            else
            {
                var map = ModelMapper<ModelT>.MapModel;
                foreach (var dataRow in result)
                    yield return map(this, _customTypeConverters, dataRow, queryCache);
            }
        }

        public void CallProcedure(SqlProcedure procedure, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            ds.Execute(procedure, parameters);
            this._ReturnDataSourceToPool(ds);
        }
    }
}