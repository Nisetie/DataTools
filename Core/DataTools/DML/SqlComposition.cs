using System.Collections.Generic;
using System.Text;

namespace DataTools.DML
{
    public class SqlComposition : SqlExpression
    {
        private List<SqlExpression> _list;

        public List<SqlExpression> Elements => _list;

        public SqlComposition(params SqlExpression[] elements) => _list = new List<SqlExpression>(elements);

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

