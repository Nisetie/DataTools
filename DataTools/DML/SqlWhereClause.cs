using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DataTools.DML
{
    public class SqlWhereClause : SqlExpression
    {

        private readonly SqlCustom _sqlEmpty = new SqlCustom("");
        private readonly SqlConstant _dummySqlConstant = new SqlConstant(1);


        private List<SqlExpression> _nodes = new List<SqlExpression>();

        public IEnumerable<SqlExpression> Nodes => _nodes;
        public SqlWhereClause AddNode(SqlExpression expression)
        {
            _nodes.Add(expression);
            return this;
        }

        /// <summary>
        /// Конструктор без параметров создает заглушку: 1 = 1.
        /// Поэтому для продолжения цепочки следующее звено надо начинать с AND.
        /// </summary>
        public SqlWhereClause()
        {
            AddNode(_dummySqlConstant);
            AddNode(new SqlEqual());
            AddNode(_dummySqlConstant);
        }

        public SqlWhereClause(SqlExpression expression)
        {
            _nodes.Add(expression);
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlWhereClause sqlWhereClause)
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

    public abstract class SqlLogicalOperator<T> : SqlExpression
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

