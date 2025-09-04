using DataTools.Commands;
using DataTools.DML;
using DataTools.Meta;
using System;
using System.Linq.Expressions;

namespace DataTools.Extensions
{
    public static class SqlWhereExtensions
    {
        private static SqlWhere _Combine(SqlWhere left, SqlExpression right)
        {
            return left.AddNode(right);
        }

        public static SqlWhere Eq(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlEqual()), right);
        public static SqlWhere Ne(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlNotEqual()), right);
        public static SqlWhere Gt(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlGreaterThan()), right);
        public static SqlWhere Ge(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlGreaterOrEqual()), right);
        public static SqlWhere Lt(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlLessThan()), right);
        public static SqlWhere Le(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlLessOrEqual()), right);
        public static SqlWhere And(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlAnd()), right);
        public static SqlWhere Or(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlOr()), right);
        public static SqlWhere Not(this SqlWhere left, SqlExpression right) => _Combine(_Combine(left, new SqlNot()), right);
        public static SqlWhere IsNull(this SqlWhere left) => _Combine(left, new SqlIsNull());

        public static SqlWhere EqValue(this SqlWhere left, object custom) => Eq(left, new SqlConstant(custom));
        public static SqlWhere NeValue(this SqlWhere left, object custom) => Ne(left, new SqlConstant(custom));
        public static SqlWhere GtValue(this SqlWhere left, object custom) => Gt(left, new SqlConstant(custom));
        public static SqlWhere GeValue(this SqlWhere left, object custom) => Ge(left, new SqlConstant(custom));
        public static SqlWhere LtValue(this SqlWhere left, object custom) => Lt(left, new SqlConstant(custom));
        public static SqlWhere LeValue(this SqlWhere left, object custom) => Le(left, new SqlConstant(custom));
        public static SqlWhere AndValue(this SqlWhere left, object custom) => And(left, new SqlConstant(custom));
        public static SqlWhere OrValue(this SqlWhere left, object custom) => Or(left, new SqlConstant(custom));

        public static SqlWhere EqName(this SqlWhere left, string colname) => Eq(left, new SqlName(colname));
        public static SqlWhere NeName(this SqlWhere left, string colname) => Ne(left, new SqlName(colname));
        public static SqlWhere GtName(this SqlWhere left, string colname) => Gt(left, new SqlName(colname));
        public static SqlWhere GeName(this SqlWhere left, string colname) => Ge(left, new SqlName(colname));
        public static SqlWhere LtName(this SqlWhere left, string colname) => Lt(left, new SqlName(colname));
        public static SqlWhere LeName(this SqlWhere left, string colname) => Le(left, new SqlName(colname));
        public static SqlWhere AndName(this SqlWhere left, string colname) => And(left, new SqlName(colname));
        public static SqlWhere OrName(this SqlWhere left, string colname) => Or(left, new SqlName(colname));

        public static SqlWhere EqPar(this SqlWhere left, SqlParameter par) => Eq(left, par);
        public static SqlWhere NePar(this SqlWhere left, SqlParameter par) => Ne(left, par);
        public static SqlWhere GtPar(this SqlWhere left, SqlParameter par) => Gt(left, par);
        public static SqlWhere GePar(this SqlWhere left, SqlParameter par) => Ge(left, par);
        public static SqlWhere LtPar(this SqlWhere left, SqlParameter par) => Lt(left, par);
        public static SqlWhere LePar(this SqlWhere left, SqlParameter par) => Le(left, par);
        public static SqlWhere AndPar(this SqlWhere left, SqlParameter par) => And(left, par);
        public static SqlWhere OrPar(this SqlWhere left, SqlParameter par) => Or(left, par);

        public static SqlWhere Name(this SqlWhere left, string colname) => _Combine(left, new SqlName(colname));
        public static SqlWhere Value(this SqlWhere left, object custom) => _Combine(left, new SqlConstant(custom));
        public static SqlWhere Param(this SqlWhere left, SqlParameter par) => _Combine(left, par);
    }
}
