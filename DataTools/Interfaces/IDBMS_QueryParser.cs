using DataTools.DML;

namespace DataTools.Interfaces
{
    public interface IDBMS_QueryParser
    {
        string ToString(SqlExpression query);
        string ToString(SqlExpression query, params SqlParameter[] parameters);
    }
}

