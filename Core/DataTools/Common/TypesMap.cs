using System.Collections.Generic;

namespace DataTools.Common
{
    public static class TypesMap
    {
        private static Dictionary<E_DBMS, Dictionary<string, DBType>> _linksToDBType = new Dictionary<E_DBMS, Dictionary<string, DBType>>();
        private static Dictionary<E_DBMS, Dictionary<int, string>> _linksToSqlType = new Dictionary<E_DBMS, Dictionary<int, string>>();

        /// <summary>
        /// Добавить прямую и обратные ассоциации между DBType и sql-типами.
        /// </summary>
        /// <param name="dbms">СУБД</param>
        /// <param name="dbtype">Общий класс родственных типов данных sql.</param>
        /// <param name="sqlType">Приоритетный для <see cref="DBType"/>> тип данных на стороне СУБД. Используется при преобразовании в тип на стороне СУБД.</param>
        /// <param name="aliases">Альтернативные типы на стороне СУБД, связанные с конкретным <see cref="DBType"/>. Используются только для преобразования в C#-типы.</param>
        public static void AddTypeLink(E_DBMS dbms, DBType dbtype, string sqlType, params string[] aliases)
        {
            if (!_linksToDBType.TryGetValue(dbms, out var linksToDBType))
                _linksToDBType[dbms] = linksToDBType = new Dictionary<string, DBType>();
            linksToDBType[sqlType] = dbtype;
            for (int i = 0; i < aliases.Length; i++) linksToDBType[aliases[i]] = dbtype;

            AddForwardLinkOnly(dbms, dbtype, sqlType);
        }

        /// <summary>
        /// Добавить только одну прямую ассоциацию от DBType к sql-типу на стороне DBMS. Не перекрывает прочие обратные ассоциации.
        /// </summary>
        /// <param name="dbms"></param>
        /// <param name="dbtype"></param>
        /// <param name="sqlType"></param>
        public static void AddForwardLinkOnly(E_DBMS dbms, DBType dbtype, string sqlType)
        {
            if (!_linksToSqlType.TryGetValue(dbms, out var linksToSqlType))
                _linksToSqlType[dbms] = linksToSqlType = new Dictionary<int, string>();
            linksToSqlType[dbtype.Id] = sqlType;
        }

        /// <summary>
        /// Получить обобщенный тип. Полезно при получении значения из СУБД.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="dbms"></param>
        /// <returns>DBType if exists else null</returns>
        public static DBType GetDBTypeFromSqlType(E_DBMS dbms, string sqlType)
        {
            if (_linksToDBType.TryGetValue(dbms, out var links))
                if (links.TryGetValue(sqlType, out var dbtype))
                    return dbtype;
            return null;
        }
        /// <summary>
        /// Получить конкретный sql-тип. Полезно для передачи значений в СУБД.
        /// </summary>
        /// <param name="dbms"></param>
        /// <param name="dbtype"></param>
        /// <returns>string if exists else null</returns>
        public static string GetSqlType(E_DBMS dbms, DBType dbtype)
        {
            if (_linksToSqlType.TryGetValue(dbms, out var links))
                if (links.TryGetValue(dbtype.Id, out var sqltype))
                    return sqltype;
            return null;
        }
    }
}

