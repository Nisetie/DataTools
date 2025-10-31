using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlComposition : SqlExpression
    {
        private List<ISqlExpression> _list;

        public IEnumerable<ISqlExpression> Elements => _list;

        /// <summary>
        /// Количество элементов в композиции
        /// </summary>
        public int Count => _list.Count;

        public SqlComposition(params ISqlExpression[] elements)
        {
            _list = new List<ISqlExpression>(elements);
            foreach (var element in _list)
                PayloadLength += element?.PayloadLength ?? 0;
        }

        public SqlComposition Add(ISqlExpression sqlExpression)
        {
            _list.Add(sqlExpression);
            PayloadLength += sqlExpression?.PayloadLength ?? 0;
            return this;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            foreach (object element in _list)
                sb.Append(element.ToString());
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (obj is SqlComposition sqlComposition)
            {
                var leftE = _list.GetEnumerator();
                var rightE = sqlComposition._list.GetEnumerator();
                while (leftE.MoveNext())
                {
                    if (!rightE.MoveNext()) return false;
                    if (!leftE.Current.Equals(rightE.Current)) return false;
                }
                return true;
            }
            return false;
        }
    }
}

