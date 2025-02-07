using DataTools.DML;

namespace DataTools.Extensions
{
    public static class SqlWhereClauseExtensions
    {
        private static SqlWhereClause _Combine(SqlWhereClause left, SqlExpression right)
        { 
            return left.AddChain(right);
        }

        public static SqlWhereClause Eq(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlEqual()), right);
        public static SqlWhereClause Ne(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlNotEqual()), right);
        public static SqlWhereClause Gt(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlGreaterThan()), right);
        public static SqlWhereClause Ge(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlGreaterOrEqual()), right);
        public static SqlWhereClause Lt(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlLesserThan()), right);
        public static SqlWhereClause Le(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlLesserOrEqual()), right);
        public static SqlWhereClause And(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlAnd()), right);
        public static SqlWhereClause Or(this SqlWhereClause left, SqlExpression right) => _Combine(_Combine(left, new SqlOr()), right);
        public static SqlWhereClause IsNull(this SqlWhereClause left) => _Combine(left, new SqlIsNull());

        public static SqlWhereClause EqValue(this SqlWhereClause left, object custom) => Eq(left, new SqlConstant(custom));
        public static SqlWhereClause NeValue(this SqlWhereClause left, object custom) => Ne(left, new SqlConstant(custom));
        public static SqlWhereClause GtValue(this SqlWhereClause left, object custom) => Gt(left, new SqlConstant(custom));
        public static SqlWhereClause GeValue(this SqlWhereClause left, object custom) => Ge(left, new SqlConstant(custom));
        public static SqlWhereClause LtValue(this SqlWhereClause left, object custom) => Lt(left, new SqlConstant(custom));
        public static SqlWhereClause LeValue(this SqlWhereClause left, object custom) => Le(left, new SqlConstant(custom));
        public static SqlWhereClause AndValue(this SqlWhereClause left, object custom) => And(left, new SqlConstant(custom));
        public static SqlWhereClause OrValue(this SqlWhereClause left, object custom) => Or(left, new SqlConstant(custom));

        public static SqlWhereClause EqName(this SqlWhereClause left, string colname) => Eq(left, new SqlName(colname));
        public static SqlWhereClause NeName(this SqlWhereClause left, string colname) => Ne(left, new SqlName(colname));
        public static SqlWhereClause GtName(this SqlWhereClause left, string colname) => Gt(left, new SqlName(colname));
        public static SqlWhereClause GeName(this SqlWhereClause left, string colname) => Ge(left, new SqlName(colname));
        public static SqlWhereClause LtName(this SqlWhereClause left, string colname) => Lt(left, new SqlName(colname));
        public static SqlWhereClause LeName(this SqlWhereClause left, string colname) => Le(left, new SqlName(colname));
        public static SqlWhereClause AndName(this SqlWhereClause left, string colname) => And(left, new SqlName(colname));
        public static SqlWhereClause OrName(this SqlWhereClause left, string colname) => Or(left, new SqlName(colname));
    }
}