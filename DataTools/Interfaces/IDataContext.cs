using DataTools.DML;
using System.Collections.Generic;

namespace DataTools.Interfaces
{
    public interface IDataContext
    {
        /// <summary>
        /// Получить объект источника данных для более гибкого обращения к данным.
        /// </summary>
        /// <returns></returns>
        IDataSource GetDataSource();

        void Execute(SqlExpression query, params SqlParameter[] parameters);
        void Execute(SqlExpression query);
        object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters);
        object ExecuteScalar(SqlExpression query);
        IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters);
        IEnumerable<object[]> ExecuteWithResult(SqlExpression query);

        /// <summary>
        /// Запрос данных из источника с автоматическим маппированием полей модели.
        /// </summary>
        /// <param name="selectBuilder"></param>
        /// <returns></returns>
        IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null, params SqlParameter[] parameters) where ModelT : class, new();
        IEnumerable<ModelT> Select<ModelT>(SqlExpression query = null) where ModelT : class, new();

        //IEnumerable<ModelT> Select<ModelT>(SqlWhereClause whereClause) where ModelT: class,new() ;

        /// <summary>
        /// Добавление экземпляра на стороне источника данных.
        /// Кроме добавления надо реализовать возврат из источника данных об экзмепляре для обновления автоматических полей в <paramref name="record"/>.
        /// </summary>
        /// <param name="record"></param>
        void Insert<ModelT>(ModelT record) where ModelT : class, new();
        /// <summary>
        /// Обновление экземпляра на стороне источника данных.
        /// Кроме обновления надо реализовать возврат из источника данных об экзмепляре для обновления автоматических полей в <paramref name="record"/>.
        /// </summary>
        /// <param name="record"></param>
        void Update<ModelT>(ModelT record) where ModelT : class, new();
        /// <summary>
        /// Удаление экземпляра на стороне источника.
        /// На стороне программы экземпляр не изменяется, что дает возможность пересоздать его заново в источнике.
        /// </summary>
        /// <param name="record"></param>
        void Delete<ModelT>(ModelT record) where ModelT : class, new();

        IEnumerable<ModelT> CallTableFunction<ModelT>(SqlFunction function, params SqlParameter[] parameters) where ModelT : class, new();
        object CallScalarFunction(SqlFunction function, params SqlParameter[] parameters);
        IEnumerable<ModelT> CallProcedure<ModelT>(SqlProcedure procedure, params SqlParameter[] parameters) where ModelT : class, new();
        void CallProcedure(SqlProcedure procedure, params SqlParameter[] parameters);
        void CreateTable<ModelT>() where ModelT : class, new();
        void DropTable<ModelT>() where ModelT : class, new();
    }
}