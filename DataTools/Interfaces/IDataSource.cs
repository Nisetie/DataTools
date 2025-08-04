using DataTools.DML;
using System.Collections.Generic;

namespace DataTools.Interfaces
{
    public interface IDataSource
    {
        void Execute(SqlExpression query, params SqlParameter[] parameters);
        object ExecuteScalar(SqlExpression query, params SqlParameter[] parameters);

        /// <summary>
        /// Запрос возвращает коллекций массивов (строк данных), где
        /// каждый элемент либо Null, либо имеет значение.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        IEnumerable<object[]> ExecuteWithResult(SqlExpression query, params SqlParameter[] parameters);
    }
}

