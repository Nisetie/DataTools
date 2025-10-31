namespace DataTools.DML
{
    public interface ISqlExpression
    {
        /// <summary>
        /// Закешированная длина полезной нагрузки sql-выражения в текстовом представлении.
        /// Чтобы промежуточные обработчики примерно понимали объем и сложность обрабатываемого запроса.
        /// </summary>
        int PayloadLength { get; }
    }

    public class SqlExpression : ISqlExpression
    {
        public int PayloadLength { get; protected set; }
    }
}

