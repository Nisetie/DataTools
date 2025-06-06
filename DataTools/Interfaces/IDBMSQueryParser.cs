using DataTools.DML;

namespace DataTools.Interfaces
{
    public interface IDBMSQueryParser
    {
        string ToString(SqlExpression query, params SqlParameter[] parameters);
    }
}

