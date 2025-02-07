using System.Collections.Generic;

namespace DataTools.DML
{
    public class SqlWhereClause : SqlExpression
    {

        private readonly SqlCustom _sqlEmpty = new SqlCustom("");
        private readonly SqlConstant _dummySqlConstant = new SqlConstant(1);


        private Queue<SqlExpression> _expressions = new Queue<SqlExpression>();

        public IEnumerable<SqlExpression> Queue => _expressions;
        public SqlWhereClause AddChain(SqlExpression expression)
        {
            _expressions.Enqueue(expression);
            return this;
        }

        /// <summary>
        /// Конструктор без параметров создает заглушку: 1 = 1.
        /// Поэтому для продолжения цепочки следующее звено надо начинать с AND.
        /// </summary>
        public SqlWhereClause()
        {
            AddChain(_dummySqlConstant);
            AddChain(new SqlEqual());
            AddChain(_dummySqlConstant);
        }

        public SqlWhereClause(SqlExpression expression)
        {
            _expressions.Enqueue(expression);
        }
    }

    public class SqlGreaterThan : SqlExpression { }
    public class SqlGreaterOrEqual : SqlExpression { }
    public class SqlLesserThan : SqlExpression { }
    public class SqlLesserOrEqual : SqlExpression { }
    public class SqlEqual : SqlExpression { }
    public class SqlNotEqual : SqlExpression { }
    public class SqlAnd : SqlExpression { }
    public class SqlOr : SqlExpression { }
    public class SqlNot : SqlExpression { }
    public class SqlIsNull : SqlExpression { }
}

