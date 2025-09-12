using DataTools.DML;
using System.Collections.Generic;

namespace DataTools.Interfaces
{
    public interface ICRUD
    {
        /// <summary>
        /// Запрос данных из источника с автоматическим маппированием полей модели.
        /// </summary>
        /// <param name="selectBuilder"></param>
        /// <returns></returns>
        IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null, params SqlParameter[] parameters) where ModelT : class, new();

        IEnumerable<dynamic> Select(IModelMetadata metadata, SqlExpression query = null, params SqlParameter[] parameters);

        /// <summary>
        /// Добавление экземпляра на стороне источника данных.
        /// Кроме добавления надо реализовать возврат из источника данных об экзмепляре для обновления автоматических полей в <paramref name="record"/>.
        /// </summary>
        /// <param name="record"></param>
        void Insert<ModelT>(ModelT record) where ModelT : class, new();
        void Insert(IModelMetadata modelMetadata, dynamic record);
        /// <summary>
        /// Обновление экземпляра на стороне источника данных.
        /// Кроме обновления надо реализовать возврат из источника данных об экзмепляре для обновления автоматических полей в <paramref name="record"/>.
        /// </summary>
        /// <param name="record"></param>
        void Update<ModelT>(ModelT record) where ModelT : class, new();
        void Update(IModelMetadata modelMetadata, dynamic record);
        /// <summary>
        /// Удаление экземпляра на стороне источника.
        /// На стороне программы экземпляр не изменяется, что дает возможность пересоздать его заново в источнике.
        /// </summary>
        /// <param name="record"></param>
        void Delete<ModelT>(ModelT record) where ModelT : class, new();
        void Delete(IModelMetadata modelMetadata, dynamic record);
    }

    public interface IDataContext :ICRUD
    {
        /// <summary>
        /// Получить объект источника данных для более гибкого обращения к данным.
        /// </summary>
        /// <returns></returns>
        IDataSource GetDataSource();

        /// <summary>
        /// Выполнить запрос с параметрами без вощвращения результата.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        void Execute(SqlExpression query, params SqlParameter[] parameters);

        object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters);

        IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters);

        IEnumerable<ModelT> CallTableFunction<ModelT>(SqlFunction function, params SqlParameter[] parameters) where ModelT : class, new();
        IEnumerable<dynamic> CallTableFunction(IModelMetadata modelMetadata, SqlFunction function, params SqlParameter[] parameters);
        object CallScalarFunction(SqlFunction function, params SqlParameter[] parameters);
        IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure, params SqlParameter[] parameters) where ModelT : class, new();
        IEnumerable<dynamic> CallProcedure(IModelMetadata modelMetadata, SqlProcedure procedure, params SqlParameter[] parameters);
        void CallProcedure(SqlProcedure procedure, params SqlParameter[] parameters);
    }
}