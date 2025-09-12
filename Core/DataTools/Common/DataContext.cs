using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

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

        public virtual IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null, params SqlParameter[] parameters) where ModelT : class, new()
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

        public virtual IEnumerable<dynamic> Select(IModelMetadata metadata, SqlExpression query = null, params SqlParameter[] parameters)
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

        public virtual void Insert<ModelT>(ModelT model) where ModelT : class, new()
        {
            var ds = this.GetDataSource();
            try
            {
                ModelMapper<ModelT>.CopyValues(
                   ReturnResultExact<ModelT>(
                       ds,
                       new SqlInsert().Into<ModelT>().Value(ModelMapper<ModelT>.GetArrayOfValues(model))
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
                    ReturnResultDynamic(
                        modelMetadata,
                        ds,
                        new SqlInsert().Into(modelMetadata).Value((object[])DynamicMapper.GetMapper(modelMetadata).GetArrayOfValues(model))
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
                    ReturnResultExact<ModelT>(
                        ds,
                        new SqlComposition(
                            new SqlUpdate().From<ModelT>().Value(ModelMapper<ModelT>.GetArrayOfValues(model)).Where(sqlWhere),
                            new SqlSelect().From<ModelT>().Where(sqlWhere)
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
                    ReturnResultDynamic(
                        modelMetadata,
                        ds,
                        new SqlComposition(
                            new SqlUpdate().From(modelMetadata).Value((object[])mapper.GetArrayOfValues(model)).Where(sqlWhere),
                            new SqlSelect().From(modelMetadata).Where(sqlWhere)
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
                return ds.ExecuteScalar(query, parameters);
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
                return ds.ExecuteWithResult(query, parameters);
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

        public void CallProcedure(SqlProcedure procedure, params SqlParameter[] parameters)
        {
            var ds = this.GetDataSource();
            ds.Execute(procedure, parameters);
            this._ReturnDataSourceToPool(ds);
        }

        protected virtual IEnumerable<dynamic> ReturnResultDynamic(IModelMetadata modelMetadata, IDataSource ds, SqlExpression query, params SqlParameter[] parameters)
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

        protected virtual IEnumerable<ModelT> ReturnResultExact<ModelT>(IDataSource ds, SqlExpression query, params SqlParameter[] parameters) where ModelT : class, new()
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