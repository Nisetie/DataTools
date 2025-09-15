using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlWhere : ISqlExpression
    {
        private List<ISqlExpression> _nodes = new List<ISqlExpression>();

        public IEnumerable<ISqlExpression> Nodes => _nodes;
        public SqlWhere AddNode(ISqlExpression expression)
        {
            _nodes.Add(expression);
            return this;
        }

        public SqlWhere()
        {
        }

        public SqlWhere(ISqlExpression expression)
        {
            _nodes.Add(expression);
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlWhere sqlWhereClause)
            {
                var leftE = _nodes.GetEnumerator();
                var rightE = sqlWhereClause._nodes.GetEnumerator();
                while (leftE.MoveNext())
                {
                    if (!rightE.MoveNext()) return false;
                    if (!leftE.Current.Equals(rightE.Current)) return false;
                }
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            return $"({string.Join(" ", _nodes)})";
        }
    }

    public abstract class SqlLogicalOperator<T> : ISqlExpression
    {
        public override bool Equals(object obj)
        {
            if (obj is SqlLogicalOperator<T> sqlLogicalOperator)
                return true;
            return false;
        }
    }

    public class SqlGreaterThan : SqlLogicalOperator<SqlGreaterThan> { public override string ToString() => ">"; }
    public class SqlGreaterOrEqual : SqlLogicalOperator<SqlGreaterOrEqual> { public override string ToString() => ">="; }
    public class SqlLessThan : SqlLogicalOperator<SqlLessThan> { public override string ToString() => "<"; }
    public class SqlLessOrEqual : SqlLogicalOperator<SqlLessOrEqual> { public override string ToString() => "<="; }
    public class SqlEqual : SqlLogicalOperator<SqlEqual> { public override string ToString() => "="; }
    public class SqlNotEqual : SqlLogicalOperator<SqlNotEqual> { public override string ToString() => "<>"; }
    public class SqlAnd : SqlLogicalOperator<SqlAnd> { public override string ToString() => "AND"; }
    public class SqlOr : SqlLogicalOperator<SqlOr> { public override string ToString() => "OR"; }
    public class SqlNot : SqlLogicalOperator<SqlNot> { public override string ToString() => "NOT"; }
    public class SqlIsNull : SqlLogicalOperator<SqlIsNull> { public override string ToString() => "IS NULL"; }
}

