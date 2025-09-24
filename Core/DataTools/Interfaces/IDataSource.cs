using DataTools.DML;
using System.Collections.Generic;

namespace DataTools.Interfaces
{
    public interface IDataSource
    {
        void Execute(ISqlExpression query, params SqlParameter[] parameters);
        void Execute(ISqlExpression query);
        object ExecuteScalar(ISqlExpression query, params SqlParameter[] parameters);
        object ExecuteScalar(ISqlExpression query);

        /// <summary>
        /// Запрос возвращает коллекций массивов (строк данных), где
        /// каждый элемент либо Null, либо имеет значение.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        IEnumerable<object[]> ExecuteWithResult(ISqlExpression query, params SqlParameter[] parameters);
        IEnumerable<object[]> ExecuteWithResult(ISqlExpression query);
    }
}

