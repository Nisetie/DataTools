using DataTools.DML;

namespace DataTools.Interfaces
{
    public interface IDBMS_QueryParser
    {
        string ToString(SqlExpression query);
        string ToString(SqlExpression query, params SqlParameter[] parameters);
        /// <summary>
        /// Упрощение дерева выражений sql-запроса до комбинации SqlCustom и SqlParameter. 
        /// Чтобы в итоге финальный текст запроса получался простой конкатенацией текста запроса и параметров.
        /// </summary>
        /// <param name="query"></param>
        /// <returns><see cref="SqlComposition"/> as <see cref="SqlExpression"/></returns>
        SqlExpression SimplifyQuery(SqlExpression query);
    }
}

