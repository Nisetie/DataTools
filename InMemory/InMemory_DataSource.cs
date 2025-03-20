using DataTools.DML;
using DataTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataTools.InMemory
{
    public class InMemory_DataSource : IDataSource
    {
        InMemory_DataContext _context;

        public void Execute(SqlExpression query)
        {
            ExecuteWithResult(query);
        }

        Stack<SqlWhereClause> wheres = new Stack<SqlWhereClause>();
        Stack<SqlExpression[]> wheresNodes = new Stack<SqlExpression[]>();
        Stack<int> wheresNodesPosition = new Stack<int>();

        Stack<Queue<Expression>> binexps = new Stack<Queue<Expression>>();
        Stack<Queue<Expression>> unaryexps = new Stack<Queue<Expression>>();
        Stack<Stack<ExpressionType>> operators = new Stack<Stack<ExpressionType>>();

        ParameterExpression param_dataRow = Expression.Parameter(typeof(object[]), "dataRow");
        ParameterExpression var_tableMetadata = Expression.Variable(typeof(IModelMetadata), "tableMetadata");
        ParameterExpression[] block_variables;

        public InMemory_DataSource() :base()
        {
            block_variables = new ParameterExpression[] { var_tableMetadata };
        }

        private void AddWhere(SqlWhereClause whereClause)
        {
            wheres.Push(whereClause);
            wheresNodes.Push(whereClause.Nodes.ToArray());
            wheresNodesPosition.Push(0);

            binexps.Push(new Queue<Expression>());
            unaryexps.Push(new Queue<Expression>());
            operators.Push(new Stack<ExpressionType>());
        }

        private Func<object[], bool> AnalyzeWhere(IModelMetadata tableMeta)
        {
            while (wheres.Count > 0)
            {
                var currentWhere = wheres.Peek();
                var currentNodes = wheresNodes.Peek();
                int ix = wheresNodesPosition.Pop();

                var current_binexprs = binexps.Peek();
                var current_unaryexps = unaryexps.Peek();
                var current_ops = operators.Peek();

                var expr = currentNodes[ix];
                switch (expr)
                {
                    case SqlWhereClause sqlWhereClause:
                        AddWhere(sqlWhereClause);
                        break;
                    case SqlConstant sqlConstant:
                        current_unaryexps.Enqueue(Expression.Convert(Expression.Constant(sqlConstant.Value), sqlConstant.Value.GetType()));
                        break;
                    case SqlName sqlName:
                        current_unaryexps.Enqueue(
                            Expression.ArrayIndex(param_dataRow, Expression.Constant(tableMeta.GetField(sqlName.ToString()).FieldOrder)));
                        break;
                    case SqlAnd sqlAnd:
                        current_ops.Push(ExpressionType.AndAlso);
                        break;
                    case SqlOr sqlOr:
                        current_ops.Push(ExpressionType.OrElse);
                        break;
                    case SqlNot sqlNot:
                        current_ops.Push(ExpressionType.Not);
                        break;
                    case SqlIsNull sqlIsNull:
                        current_unaryexps.Enqueue(Expression.Constant(null));
                        current_ops.Push(ExpressionType.Equal);
                        break;
                    case SqlGreaterOrEqual sqlGreaterOrEqual:
                        current_ops.Push(ExpressionType.GreaterThanOrEqual);
                        break;
                    case SqlGreaterThan greaterThan:
                        current_ops.Push(ExpressionType.GreaterThan);
                        break;
                    case SqlLessOrEqual sqlLesserOrEqual:
                        current_ops.Push(ExpressionType.LessThanOrEqual);
                        break;
                    case SqlLessThan sqlLesserThan:
                        current_ops.Push(ExpressionType.LessThan);
                        break;
                    case SqlEqual sqlEqual:
                        current_ops.Push(ExpressionType.Equal);
                        break;
                    case SqlNotEqual sqlNotEqual:
                        current_ops.Push(ExpressionType.NotEqual);
                        break;
                    default: break;
                }

                ix++;

                if (current_unaryexps.Count == 2)
                {
                    var left = current_unaryexps.Dequeue();
                    var right = current_unaryexps.Dequeue();
                    var op = current_ops.Pop();
                    var valueType = left.Type == typeof(object) ? right.Type : left.Type;
                    current_binexprs.Enqueue(Expression.MakeBinary(op, Expression.Convert(left, valueType), Expression.Convert(right, valueType)));
                }
                if (current_binexprs.Count == 2)
                {
                    var left = current_binexprs.Dequeue();
                    var right = current_binexprs.Dequeue();
                    var op = current_ops.Pop();
                    current_binexprs.Enqueue(Expression.MakeBinary(op, left, right));
                }

                if (ix >= currentNodes.Length)
                {
                    wheres.Pop();
                    wheresNodes.Pop();
                    if (binexps.Count > 1)
                    {
                        var binexp = binexps.Pop().Dequeue();
                        binexps.Peek().Enqueue(binexp);
                    }
                    unaryexps.Pop();
                    operators.Pop();
                }
                else wheresNodesPosition.Push(ix);
            }

            return Expression.Lambda<Func<object[], bool>>(
                Expression.Block(
                    variables: block_variables
                    , Expression.Assign(var_tableMetadata, Expression.Constant(tableMeta))
                    , binexps.Pop().Dequeue()
                    )
                , param_dataRow).Compile();
        }

        public object ExecuteScalar(SqlExpression query)
        {
            return ExecuteWithResult(query).First()[0];
        }

        public IEnumerable<object[]> ExecuteWithResult(SqlExpression query)
        {
            binexps.Clear();
            unaryexps.Clear();
            operators.Clear();
            this.wheres.Clear();
            this.wheresNodes.Clear();
            this.wheresNodesPosition.Clear();

            string tableName;
            IModelMetadata tableMeta;
            IList<object[]> tableData;

            if (query is SqlInsert sqlInsert)
            {
                tableName = sqlInsert.IntoDestination.ToString();
                _context.LockTable(tableName);
                tableMeta = _context.GetTableMetadata(tableName);
                tableData = _context.GetData(tableName);
                var dataRow = new object[tableMeta.FieldsCount];
                int i = 0;
                var col_e = sqlInsert.Columns.GetEnumerator();
                var val_e = sqlInsert.Values.GetEnumerator();
                while (col_e.MoveNext())
                {
                    val_e.MoveNext();
                    if (val_e.Current is SqlConstant sqlConstant)
                        dataRow[tableMeta.GetField(col_e.Current.Name).FieldOrder] = sqlConstant.Value;
                    else
                        dataRow[tableMeta.GetField(col_e.Current.Name).FieldOrder] = val_e.Current.ToString();
                }
                tableData.Add(dataRow);
                _context.UnlockTable(tableName);
                return new object[1][] { dataRow };

            }
            else if (query is SqlUpdate sqlUpdate)
            {
                tableName = sqlUpdate.FromSource.ToString();
                _context.LockTable(tableName);
                tableMeta = _context.GetTableMetadata(tableName);
                tableData = _context.GetData(tableName);
                AddWhere(sqlUpdate.Wheres);
                var whereLambda = AnalyzeWhere(tableMeta);
                var col_e = sqlUpdate.Columns.GetEnumerator();
                var val_e = sqlUpdate.Values.GetEnumerator();
                var f = tableData.Where(whereLambda).ToArray();
                while (col_e.MoveNext() && val_e.MoveNext())
                {
                    object value = val_e.Current;
                    if (value is SqlConstant sqlConstant) value = sqlConstant.Value; else value = value.ToString();
                    foreach (var el in f)
                        el[tableMeta.GetField(col_e.Current.Name).FieldOrder] = value;

                }
                _context.UnlockTable(tableName);
                return f;
            }
            else if (query is SqlDelete sqlDelete)
            {
                tableName = sqlDelete.FromSource.ToString();
                _context.LockTable(tableName);
                tableMeta = _context.GetTableMetadata(tableName);
                tableData = _context.GetData(tableName);
                AddWhere(sqlDelete.Wheres);
                var whereLambda = AnalyzeWhere(tableMeta);
                var r = tableData.Where(whereLambda).ToArray();
                foreach (var el in r) tableData.Remove(el);
                _context.UnlockTable(tableName);
            }
            else if (query is SqlSelect sqlSelect)
            {
                tableName = sqlSelect.FromSource.ToString();
                _context.LockTable(tableName);
                tableMeta = _context.GetTableMetadata(tableName);
                tableData = _context.GetData(tableName);
                AddWhere(sqlSelect.Wheres);
                var whereLambda = AnalyzeWhere(tableMeta);
                var f = tableData.Where(whereLambda).ToArray();
                _context.UnlockTable(tableName);
                return f;
            }
            return null;
        }

        public void Initialize(IDataContext dataContext)
        {
            _context = dataContext as InMemory_DataContext;
        }
    }
}
