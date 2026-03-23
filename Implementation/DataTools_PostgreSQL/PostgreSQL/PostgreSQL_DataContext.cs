using DataTools.Common;
using DataTools.Interfaces;

namespace DataTools.PostgreSQL
{
    public class PostgreSQL_DataContext : DataContext
    {
        public string ConnectionString { get; set; }

        static PostgreSQL_DataContext()
        {

        }

        public PostgreSQL_DataContext() : base()
        {

            // DateTimeOffset.Parse() возвращает UTC время так, как будто это время в локальном часовом поясе
            // поэтому пришлось применить этот костыль, чтобы получать все-таки UTC+0 формат
            //this.AddCustomTypeConverter<DateTimeOffset>(o => DateTimeOffset.Parse(o.ToString(), null, DateTimeStyles.AssumeUniversal));
        }

        public PostgreSQL_DataContext(string connectionString) : base()
        {
            this.ConnectionString = connectionString;
        }

        protected override IDataSource _GetDataSource()
        {
            var ds = new PostgreSQL_DataSource(ConnectionString);
            return ds;
        }
    }
}