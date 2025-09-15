using DataTools.DML;
using DataTools.Interfaces;

namespace DataTools.Deploy
{
    public abstract class DeployerBase
    {
        public abstract ISqlExpression GetRestoreIdentityQuery(IModelMetadata metadata);
    }
}
