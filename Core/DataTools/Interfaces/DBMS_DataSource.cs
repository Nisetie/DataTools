using DataTools.Interfaces;

namespace DataTools.Common
{
    /// <summary>
    /// Обертка вокруг специфической реализации ISQLConnection
    /// </summary>
    public abstract class DBMS_DataSource : DataSource
    {
        protected IDBMS_QueryParser _queryParser;

        public DBMS_DataSource(IDBMS_QueryParser queryParser) { _queryParser = queryParser; }
    }
}
