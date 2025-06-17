using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace DataTools.Commands
{
    public sealed class SelectCommmand<ModelT> where ModelT : class, new()
    {
        public IDataContext Context { get; set; }
        public SqlSelect Query { get; set; } = new SqlSelect().From<ModelT>();

        public SelectCommmand(IDataContext context) { Context = context; }

        public IEnumerable<ModelT> Select(params SqlParameter[] parameters)
        {
            return Context.Select<ModelT>(Query, parameters);
        }

        public SelectCommmand<ModelT> Where(string columnName, object value)
        {
            if (value == null)
                Query.Where(new SqlWhereClause(new SqlName(columnName)).IsNull());
            else
                Query.Where(new SqlWhereClause(new SqlName(columnName)).Eq(new SqlConstant(value)));
            return this;
        }

        public SelectCommmand<ModelT> Where(SqlWhereClause whereClause)
        {
            Query.Where(whereClause);
            return this;
        }

        public SelectCommmand<ModelT> OrderBy(params string[] columnName)
        {
            Query.OrderBy((from c in columnName select new SqlOrderByClause(new SqlName(c))).ToArray());
            return this;
        }

        public SelectCommmand<ModelT> Offset(SqlExpression offset)
        {
            Query.Offset(offset);
            return this;
        }
        public SelectCommmand<ModelT> Limit(SqlExpression limit)
        {
            Query.Limit(limit);
            return this;
        }

        public override string ToString()
        {
            return Query.ToString();
        }
    }
}