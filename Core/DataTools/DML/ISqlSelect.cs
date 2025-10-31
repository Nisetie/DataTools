using System.Collections.Generic;

namespace DataTools.DML
{
    public interface ISqlSelect<out T>
        : ISqlExpression
        where T : ISqlExpression, ISqlSelect<T>
    {
        ISqlExpression FromSource { get; }
        ISqlExpression LimitRows { get; }
        ISqlExpression OffsetRows { get; }
        IEnumerable<SqlOrderByClause> Orders { get; }
        IEnumerable<ISqlExpression> Selects { get; }
        SqlWhere Wheres { get; }
        T From(ISqlExpression objectName);
        T Where(SqlWhere where);
        T OrderBy(params SqlOrderByClause[] order);
        T Offset(ISqlExpression offset);
        T Limit(ISqlExpression limit);
        T Select(params ISqlExpression[] selects);
    }
}

