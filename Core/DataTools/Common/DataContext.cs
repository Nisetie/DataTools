using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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
        public IEnumerable<ModelT> Select<ModelT>(ISqlExpression query = null) where ModelT : class, new()
            => Select<ModelT>(query, null);
        public virtual IEnumerable<ModelT> Select<ModelT>(ISqlExpression query = null, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                if (query == null) query = new SqlSelect().From<ModelT>().Select<ModelT>();
                foreach (var row in GetResultExact<ModelT>(ds, query, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<dynamic> Select(IModelMetadata metadata, ISqlExpression query = null)
            => Select(metadata, query, null);
        public virtual IEnumerable<dynamic> Select(IModelMetadata metadata, ISqlExpression query = null, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                if (query == null) query = new SqlSelect().From(metadata).Select(metadata);
                foreach (var row in GetResultDynamic(metadata, ds, query, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public virtual void Insert<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                ModelMapper<ModelT>.CopyValues(
                   GetResultExact<ModelT>(
                       ds,
                       new SqlInsert().Into<ModelT>().Value(ModelMetadata<ModelT>.Instance, ModelMapper<ModelT>.GetArrayOfValues(model))
                       ).Last(),
                   model);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }
        public virtual void Insert(IModelMetadata modelMetadata, dynamic model)
        {
            var ds = this.GetDataSource();
            try
            {
                DynamicMapper.CopyValues(
                    modelMetadata,
                    GetResultDynamic(
                        modelMetadata,
                        ds,
                        new SqlInsert().Into(modelMetadata).Value(modelMetadata, (object[])DynamicMapper.GetMapper(modelMetadata).GetArrayOfValues(model))
                        ).Last(),
                    model);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public virtual void Update<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                var sqlWhere = ModelMapper<ModelT>.GetWhereClause(model);
                ModelMapper<ModelT>.CopyValues(
                    GetResultExact<ModelT>(
                        ds,
                        new SqlComposition(
                            new SqlUpdate().From<ModelT>().Value(ModelMetadata<ModelT>.Instance, ModelMapper<ModelT>.GetArrayOfValues(model)).Where(sqlWhere),
                            new SqlSelect().From<ModelT>().Select<ModelT>().Where(sqlWhere)
                            )
                        ).Last(),
                    model);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public virtual void Update(IModelMetadata modelMetadata, dynamic model)
        {
            var ds = this.GetDataSource();
            try
            {
                var mapper = DynamicMapper.GetMapper(modelMetadata);
                SqlWhere sqlWhere = mapper.GetWhereClause(model);
                DynamicMapper.CopyValues(
                    modelMetadata,
                    GetResultDynamic(
                        modelMetadata,
                        ds,
                        new SqlComposition(
                            new SqlUpdate().From(modelMetadata).Value(modelMetadata, (object[])mapper.GetArrayOfValues(model)).Where(sqlWhere),
                            new SqlSelect().From(modelMetadata).Select(modelMetadata).Where(sqlWhere)
                            )
                        ).Last(),
                    model);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public virtual void Delete<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                ds.Execute(
                    new SqlDelete()
                    .From<ModelT>()
                    .Where(ModelMapper<ModelT>.GetWhereClause(model))
                    );
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }
        public virtual void Delete(IModelMetadata modelMetadata, dynamic model)
        {
            var ds = this.GetDataSource();
            try
            {
                ds.Execute(new SqlDelete()
                    .From(modelMetadata)
                    .Where(DynamicMapper.GetMapper(modelMetadata).GetWhereClause(model)));
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void Execute(ISqlExpression query) => Execute(query, null);
        public void Execute(ISqlExpression query, params SqlParameter[] parameters)
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

        public object ExecuteScalar(ISqlExpression query) => ExecuteScalar(query, null);
        public object ExecuteScalar(ISqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                return ds.ExecuteScalar(query, parameters);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<object[]> ExecuteWithResult(ISqlExpression query) => ExecuteWithResult(query, null);
        public IEnumerable<object[]> ExecuteWithResult(ISqlExpression query, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                return ds.ExecuteWithResult(query, parameters);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<ModelT> CallTableFunction<ModelT>
            (SqlFunction function)
            where ModelT : class, new()
            => CallTableFunction<ModelT>(function, null);
        public IEnumerable<ModelT> CallTableFunction<ModelT>
            (SqlFunction function, params SqlParameter[] parameters)
            where ModelT : class, new()
            => Select<ModelT>(new SqlSelect().From(function, "f").Select(ModelMetadata<ModelT>.Instance), parameters);
        public IEnumerable<dynamic> CallTableFunction
            (IModelMetadata modelMetadata, SqlFunction function)
            => CallTableFunction(modelMetadata, function, null);
        public IEnumerable<dynamic> CallTableFunction
            (IModelMetadata modelMetadata, SqlFunction function, params SqlParameter[] parameters)
            => Select(modelMetadata, new SqlSelect().From(function, "f").Select(modelMetadata), parameters);
        public object CallScalarFunction
            (SqlFunction function)
            => CallScalarFunction(function, null);
        public object CallScalarFunction
            (SqlFunction function, params SqlParameter[] parameters)
            => ExecuteScalar(new SqlSelect().Select(function), parameters);

        public IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure)
            where ModelT : class, new()
            => CallProcedure<ModelT>(procedure, null);
        public IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                foreach (var row in GetResultExact<ModelT>(ds, procedure, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public IEnumerable<dynamic> CallProcedure(IModelMetadata modelMetadata, SqlProcedure procedure)
            => CallProcedure(modelMetadata, procedure, null);
        public IEnumerable<dynamic> CallProcedure(IModelMetadata modelMetadata, SqlProcedure procedure, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                foreach (var row in GetResultDynamic(modelMetadata, ds, procedure, parameters))
                    yield return row;
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        public void CallProcedure(SqlProcedure procedure) => CallProcedure(procedure, null);
        public void CallProcedure(SqlProcedure procedure, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            try
            {
                ds.Execute(procedure, parameters);
            }
            finally
            {
                this._ReturnDataSourceToPool(ds);
            }
        }

        protected IEnumerable<dynamic> GetResultDynamic
            (IModelMetadata modelMetadata, IDataSource ds, ISqlExpression query)
            => GetResultDynamic(modelMetadata, ds, query, null);
        protected virtual IEnumerable<dynamic> GetResultDynamic(IModelMetadata modelMetadata, IDataSource ds, ISqlExpression query, params SqlParameter[] parameters)
        {
            var result = ds.ExecuteWithResult(query, parameters);
            var queryCache = new SelectCache();
            var map = DynamicMapper.GetMapper(modelMetadata).MapObjectArrayToModel;
            foreach (var dataRow in result)
            {
                var model = new DynamicModel(modelMetadata);
                map(model, this, null, dataRow, queryCache);
                yield return model;
            }
        }
        protected IEnumerable<ModelT> GetResultExact<ModelT>(IDataSource ds, ISqlExpression query)
            where ModelT : class, new()
            => GetResultExact<ModelT>(ds, query, null);
        protected virtual IEnumerable<ModelT> GetResultExact<ModelT>(IDataSource ds, ISqlExpression query, params SqlParameter[] parameters) where ModelT : class, new()
        {
            var result = ds.ExecuteWithResult(query, parameters);
            var queryCache = new SelectCache();
            var map = ModelMapper<ModelT>.MapObjectArrayToModel;
            foreach (var dataRow in result)
            {
                var model = new ModelT();
                map(model, this, null, dataRow, queryCache);
                yield return model;
            }
        }
    }
}