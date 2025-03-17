using System.Collections.Generic;

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
    }

    public abstract class SqlLogicalOperator : SqlExpression { }

    public class SqlGreaterThan : SqlLogicalOperator { }
    public class SqlGreaterOrEqual : SqlLogicalOperator { }
    public class SqlLessThan : SqlLogicalOperator { }
    public class SqlLessOrEqual : SqlLogicalOperator { }
    public class SqlEqual : SqlLogicalOperator { }
    public class SqlNotEqual : SqlLogicalOperator { }
    public class SqlAnd : SqlLogicalOperator { }
    public class SqlOr : SqlLogicalOperator { }
    public class SqlNot : SqlLogicalOperator { }
    public class SqlIsNull : SqlLogicalOperator { }
}

