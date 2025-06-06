using DataTools.Interfaces;

namespace DataTools.Common
{
    public abstract class DBMS_DataSource : DataSource
    {
        protected IDBMSQueryParser _queryParser;

        public DBMS_DataSource(IDBMSQueryParser queryParser) { _queryParser = queryParser; }
    }
}
