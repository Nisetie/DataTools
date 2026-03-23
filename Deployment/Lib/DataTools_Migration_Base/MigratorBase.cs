using DataTools.DML;
using DataTools.Interfaces;

namespace DataTools.Deploy
{
    public abstract class MigratorBase
    {
        public abstract void SetupModel(IDataContext dataContext, IModelMetadata modelMetadata);

        /// <summary>
        /// Получить команду очистки таблицы.
        /// В разных СУБД это действие может выполняться по-разному. Где-то только через DELETE, где-то через TRUNCATE.
        /// </summary>
        /// <param name="modelMetadata"></param>
        /// <returns></returns>
        public abstract ISqlExpression GetClearTableQuery();

        /// <summary>
        /// Подготовительные действия перед КАЖДОЙ порцией переносимых данных. Например, временное отключение работы автоинкрементов.
        /// </summary>
        /// <param name="modelMetadata"></param>
        /// <returns></returns>
        public abstract ISqlExpression GetBeforeMigrationQuery();

        /// <summary>
        /// Финальные действия после миграции данных. Например, включение автоинкрементов.
        /// </summary>
        /// <param name="modelMetadata"></param>
        /// <returns></returns>
        public abstract ISqlExpression GetAfterMigrationQuery();
    }
}
