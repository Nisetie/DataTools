using DataTools.DML;
using DataTools.Interfaces;

namespace DataTools.Deploy
{
    public abstract class DeployerBase
    {
        public abstract SqlExpression GetRestoreIdentityQuery(IModelMetadata metadata);
    }
}
