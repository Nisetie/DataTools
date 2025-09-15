using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Common
{
    /// <summary>
    /// Оболочка над IDataContext. 
    /// Для кастомизации преобразования в отдельные целевые типы данных, в отдельные целевые модели данных.
    /// Для кастомизации выполнения SELECT, INSERT, DELETE, UPDATE для определенных моделей данных (их метамоделей).
    /// </summary>
    public sealed class DataContextCustomizable : DataContext
    {
        public IDataContext DataContext { get; }
        public DataContextCustomizable(IDataContext dataContext)
        {
            DataContext = dataContext;
        }
        private Dictionary<string, Func<IDataContext, IEnumerable<object>>> _customModelSelects = new Dictionary<string, Func<IDataContext, IEnumerable<object>>>();
        private Dictionary<string, Action<object, IDataContext>> _customModelInserts = new Dictionary<string, Action<object, IDataContext>>();
        private Dictionary<string, Action<object, IDataContext>> _customModelUpdates = new Dictionary<string, Action<object, IDataContext>>();
        private Dictionary<string, Action<object, IDataContext>> _customModelDeletes = new Dictionary<string, Action<object, IDataContext>>();

        private Dictionary<string, Action<object, IDataContext, object[]>> _customModelMappers = new Dictionary<string, Action<object, IDataContext, object[]>>();

        private Dictionary<Type, Func<object, object>> _customTypeConverters = new Dictionary<Type, Func<object, object>>();

        protected override IDataSource _GetDataSource() => DataContext.GetDataSource();

        /// <summary>
        /// Добавить произвольное преобразование сырых данных в модель типа <typeparamref name="ModelT"/>.
        /// </summary>
        /// <typeparam name="ModelT"></typeparam>
        /// <param name="mapper">Первый аргумент метода - экземпляр <typeparamref name="ModelT"/>. Для работы с моделью используйте приведение типа.</param>
        public void AddCustomModelMapper<ModelT>(Action<object, IDataContext, object[]> mapper) where ModelT : class, new() => AddCustomModelMapper(mapper, ModelMetadata<ModelT>.Instance);
        /// <summary>
        /// Добавить произвольное преобразование сырых данных в модель, имеющую имя IModelMetadata.ModelName.
        /// </summary>
        /// <param name="mapper">Первый аргумент метода - экземпляр модели. Для работы с моделью используйте приведение типа.</param>
        /// <param name="metadata"></param>
        public void AddCustomModelMapper(Action<object, IDataContext, object[]> mapper, IModelMetadata metadata) => _customModelMappers[metadata.ModelName] = mapper;
        private Action<object, IDataContext, object[]> GetCustomModelMapper<ModelT>() where ModelT : class, new() => GetCustomModelMapper(ModelMetadata<ModelT>.Instance);
        private Action<object, IDataContext, object[]> GetCustomModelMapper(IModelMetadata metadata) => _customModelMappers.TryGetValue(metadata.ModelName, out var value) ? value : null;
        public void RemoveCustomModelMapper<ModelT>() where ModelT : class, new() => RemoveCustomModelMapper(ModelMetadata<ModelT>.Instance);
        public void RemoveCustomModelMapper(IModelMetadata metadata) => _customModelMappers.Remove(metadata.ModelName);
        public void AddCustomTypeConverter<T>(Func<object, object> converter) => _customTypeConverters[typeof(T)] = converter;
        public void RemoveCustomTypeConverter<T>() => _customTypeConverters.Remove(typeof(T));

        public void AddCustomModelSelect
            (IModelMetadata metadata, Func<IDataContext, IEnumerable<object>> func)
            => _customModelSelects[metadata.ModelName] = func;
        public void RemoveCustomModelSelect
            (IModelMetadata metadata)
            => _customModelSelects.Remove(metadata.ModelName);
        public void AddCustomModelInsert
            (IModelMetadata metadata, Action<object, IDataContext> act)
            => _customModelInserts[metadata.ModelName] = act;
        public void RemoveCustomModelInsert
            (IModelMetadata metadata)
            => _customModelInserts.Remove(metadata.ModelName);
        public void AddCustomModelUpdate
            (IModelMetadata metadata, Action<object, IDataContext> act)
            => _customModelUpdates[metadata.ModelName] = act;
        public void RemoveCustomModelUpdate
            (IModelMetadata metadata)
            => _customModelUpdates.Remove(metadata.ModelName);
        public void AddCustomModelDelete
            (IModelMetadata metadata, Action<object, IDataContext> act)
            => _customModelDeletes[metadata.ModelName] = act;
        public void RemoveCustomModelDelete
            (IModelMetadata metadata)
            => _customModelDeletes.Remove(metadata.ModelName);
        public override IEnumerable<ModelT> Select<ModelT>(ISqlExpression query = null, params SqlParameter[] parameters)
        {
            if (_customModelSelects.TryGetValue(ModelMetadata<ModelT>.Instance.ModelName, out var func))
                return func(DataContext).Select(obj => obj as ModelT);
            else
                return base.Select<ModelT>(query, parameters);
        }
        public override IEnumerable<dynamic> Select(IModelMetadata metadata, ISqlExpression query = null, params SqlParameter[] parameters)
        {
            if (_customModelSelects.TryGetValue(metadata.ModelName, out var func))
                return func(DataContext).Select(obj => (dynamic)obj);
            else
                return base.Select(metadata, query, parameters);
        }
        public override void Insert<ModelT>(ModelT record)
        {
            if (_customModelInserts.TryGetValue(ModelMetadata<ModelT>.Instance.ModelName, out var act))
                act(record, DataContext);
            else
                base.Insert(record);
        }
        public override void Insert(IModelMetadata modelMetadata, dynamic record)
        {
            if (_customModelInserts.TryGetValue(modelMetadata.ModelName, out var act))
                act(record, DataContext);
            else
                base.Insert(modelMetadata, (object)record);
        }
        public override void Update<ModelT>(ModelT record)
        {
            if (_customModelUpdates.TryGetValue(ModelMetadata<ModelT>.Instance.ModelName, out var act))
                act(record, DataContext);
            else
                base.Update(record);
        }
        public override void Update(IModelMetadata modelMetadata, dynamic record)
        {
            if (_customModelUpdates.TryGetValue(modelMetadata.ModelName, out var act))
                act(record, DataContext);
            else
                base.Update(modelMetadata, (object)record);
        }
        public override void Delete<ModelT>(ModelT record)
        {
            if (_customModelDeletes.TryGetValue(ModelMetadata<ModelT>.Instance.ModelName, out var act))
                act(record, DataContext);
            else
                base.Delete(record);
        }

        protected override IEnumerable<ModelT> GetResultExact<ModelT>(IDataSource ds, ISqlExpression query, params SqlParameter[] parameters)
        {
            var result = ds.ExecuteWithResult(query, parameters);
            var queryCache = new SelectCache();
            var customMapper = GetCustomModelMapper(ModelMetadata<ModelT>.Instance);
            if (customMapper != null)
                foreach (var dataRow in result)
                {
                    ModelT model = new ModelT();
                    customMapper(model, this, dataRow);
                    yield return model;
                }
            else
            {
                var map = ModelMapper<ModelT>.MapObjectArrayToModel;
                foreach (var dataRow in result)
                {
                    var model = new ModelT();
                    map(model, this, _customTypeConverters.Count == 0 ? null : _customTypeConverters, dataRow, queryCache);
                    yield return model;
                }
            }
        }
        protected override IEnumerable<dynamic> GetResultDynamic(IModelMetadata modelMetadata, IDataSource ds, ISqlExpression query, params SqlParameter[] parameters)
        {
            var result = ds.ExecuteWithResult(query, parameters);
            var queryCache = new SelectCache();

            var customMapper = GetCustomModelMapper(modelMetadata);
            if (customMapper != null)
                foreach (var dataRow in result)
                {
                    var model = new DynamicModel(modelMetadata);
                    customMapper(model, this, dataRow);
                    yield return model;
                }
            else
            {
                var map = DynamicMapper.GetMapper(modelMetadata).MapObjectArrayToModel;
                foreach (var dataRow in result)
                {
                    var model = new DynamicModel(modelMetadata);
                    map(model, this, _customTypeConverters.Count == 0 ? null : _customTypeConverters, dataRow, queryCache);
                    yield return model;
                }
            }
        }

    }
}