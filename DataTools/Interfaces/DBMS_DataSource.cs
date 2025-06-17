using DataTools.Interfaces;

namespace DataTools.Common
{
    public abstract class DBMS_DataSource : DataSource
    {
        protected IDBMS_QueryParser _queryParser;

        public DBMS_DataSource(IDBMS_QueryParser queryParser) { _queryParser = queryParser; }
    }
}
