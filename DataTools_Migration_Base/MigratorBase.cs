using DataTools.DML;
using DataTools.Interfaces;

namespace DataTools.Deploy
{
    public abstract class MigratorBase
    {
        /// <summary>
        /// Получить команду очистки таблицы.
        /// В разных СУБД это действие может выполняться по-разному. Где-то только через DELETE, где-то через TRUNCATE.
        /// </summary>
        /// <param name="modelMetadata"></param>
        /// <returns></returns>
        public abstract SqlExpression GetClearTableQuery(IModelMetadata modelMetadata);

        /// <summary>
        /// Подготовительные действия. Например, отключение автоинкрементов.
        /// </summary>
        /// <param name="modelMetadata"></param>
        /// <returns></returns>
        public abstract SqlExpression BeforeMigration(IModelMetadata modelMetadata);

        /// <summary>
        /// Действия после миграции данных. Например, включение автоинкрементов.
        /// </summary>
        /// <param name="modelMetadata"></param>
        /// <returns></returns>
        public abstract SqlExpression AfterMigration(IModelMetadata modelMetadata);
    }
}
