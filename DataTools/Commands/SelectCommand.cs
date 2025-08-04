using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System.Collections.Generic;
namespace DataTools.Commands
{
    public interface ISelectCommand<SelectCommandT>
    {
        IDataContext Context { get; set; }
        IModelMetadata Metadata { get; set; }
        SqlSelect Query { get; set; }

        SelectCommandT Limit(SqlExpression limit);
        SelectCommandT Offset(SqlExpression offset);
        SelectCommandT OrderBy(params string[] columnNames);
        SelectCommandT Where(SqlWhere whereClause);
        SelectCommandT Where(string columnName, object value);
    }

    public interface ISelectCommandDynamicResult{
        IEnumerable<dynamic> Select();
        IEnumerable<dynamic> Select(params SqlParameter[] parameters);
    }

    public interface ISelectCommandExactResult<ModelT> where ModelT : class, new()
    {
        IEnumerable<ModelT> Select();

        IEnumerable<ModelT> Select(params SqlParameter[] parameters);
    }

    public abstract class SelectCommandBase<SelectCommandT> : ISelectCommand<SelectCommandT> where SelectCommandT: SelectCommandBase<SelectCommandT>
    {
        public IDataContext Context { get; set; }
        public IModelMetadata Metadata { get; set; }
        public SqlSelect Query { get; set; }

        public SelectCommandBase(IDataContext context, IModelMetadata metadata)
        {
            Context = context;
            Metadata = metadata;
            Query = new SqlSelect().From(metadata);
        }

        public SelectCommandT Where(string columnName, object value)
        {
            if (value == null)
                Query.Where(new SqlWhere(new SqlName(columnName)).IsNull());
            else
                Query.Where(new SqlWhere(new SqlName(columnName)).Eq(new SqlConstant(value)));
            return this as SelectCommandT;
        }

        public SelectCommandT Where(SqlWhere whereClause)
        {
            Query.Where(whereClause);
            return this as SelectCommandT;
        }

        public SelectCommandT OrderBy(params string[] columnNames)
        {
            var orders = new SqlOrderByClause[columnNames.Length];
            for (int i = 0; i < orders.Length; i++)
                orders[i] = new SqlOrderByClause(new SqlName(columnNames[i]));
            Query.OrderBy(orders);
            return this as SelectCommandT;
        }

        public SelectCommandT Offset(SqlExpression offset)
        {
            Query.Offset(offset);
            return this as SelectCommandT;
        }
        public SelectCommandT Limit(SqlExpression limit)
        {
            Query.Limit(limit);
            return this as SelectCommandT;
        }

        public override string ToString()
        {
            return Query.ToString();
        }
    }

    public class SelectCommand : SelectCommandBase<SelectCommand>, ISelectCommandDynamicResult
    {
        public SelectCommand(IDataContext context, IModelMetadata metadata) : base(context, metadata)
        {
        }

        public IEnumerable<dynamic> Select() => Context.Select(Metadata, Query);
        public IEnumerable<dynamic> Select(params SqlParameter[] parameters) => Context.Select(Metadata, Query, parameters);        
    }

    public class SelectCommand<ModelT> : SelectCommandBase<SelectCommand<ModelT>>, ISelectCommandExactResult<ModelT> where ModelT : class, new()
    {
        public SelectCommand(IDataContext context) :base(context, ModelMetadata<ModelT>.Instance)  { }

        public IEnumerable<ModelT> Select() => Context.Select<ModelT>(Query);

        public IEnumerable<ModelT> Select(params SqlParameter[] parameters) => Context.Select<ModelT>(Query, parameters);
    }
}