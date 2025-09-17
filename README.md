# DataTools. Ещё одно ORM.

Преобразование данных, получаемых из компонентов ADO.NET в модели (классы) и обратно. Основные приемы и технологии:
- минимальное использование рефлексии для предварительного составления метамоделей сущностей
- Expression Tree для подготовки функций маппинга и генерации CRUD-команд

Возможности:
- преобразование информации из данных IDataReader в экземпляры классов или динамические объекты
- поддержка рекурсивных связей при выполнении запросов SELECT
- поддержка работы с dynamic-типами моделей
- автоматическое преобразование свойств моделей (классов) в строковые литералы при передаче CRUD-команд на сторону СУБД
- конструирование DML-команд и запросов
- поддержка подставляемых параметров в sql-запросах
- частичная поддержка формирования DDL-команд
- генерация метаданных, их разворачивание и миграция данных из одной СУБД в другую
- расширяемость поддержки различных СУБД

Недостатки и замечания:
- по производительности простые тесты на выборку выдают скорости где-то между EntityFramework и Dapper
- нет поддержки JOIN'ов на уровне конструктора запросов
- кеширование моделей выполняется только во и на время работы SELECT'ов для исключения повторных запросов уже полученных записей

Реализована работа с СУБД:
- MSSQL
- PostgreSQL
- SQLite в режимах file и in-memory

Начало создания: осень-зима 2024г.

## Основные компоненты

### Библиотека DataTools
Ядро системы, включающее все базовые классы, интерфейсы и классы расширений для развития функционала.

### Библиотека DataTools_MSSQL
Реализация взаимодействия с СУБД MSSQL.

### Библиотека DataTools_PostgreSQL
Реализация взаимодействия с СУБД PostgreSQL.

### Библиотека DataTools_SQLite
Реализация взаимодействия с БД SQLite.

### Библиотека DataTools_InMemory_SQLite
Реализация взаимодействия с БД SQLite только в режиме in-memory.

### Библиотека DataTools_GeneratorLib
Основная библиотека сбора метаданных из СУБД

### Консольное приложение DataTools_Generator
Утилита для анализа и созранения описаний моделей данных.

### Библиотека DataTools_DeployerLib
Основная библиотека для разворачивания метамоделей в целевой СУБД.

### Консольное приложение DataTools_Deployer
Утилита для разворачивания метамоделей в целевой СУБД. Использовать после генерации метамоделей.

### Библиотека DataTools_DataMigrationLib
Основная библиотека для переноса данных из одной СУБД в другую.

### Консольное приложение DataTools_DataMigration
Утилита для переноса данных из одной СУБД в другую. Использовать после разворачивания метамоделей.

## Применение.

### Работа с данными.

#### Описание модели данных.

Для пример в СУБД MSSQL есть таблица:

```
  CREATE TABLE [dbo].[TestModel](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[LongId] [bigint] NULL,
	[ShortId] [smallint] NULL,
	[Name] [nvarchar](max) NULL,
	[CharCode] [nvarchar](max) NULL,
	[Checked] [bit] NULL,
	[Value] [int] NULL,
	[FValue] [real] NULL,
	[GValue] [float] NULL,
	[Money] [decimal](38, 19) NULL,
	[Timestamp] [datetime] NULL,
	[Duration] [time](7) NULL,
	[Guid] [uniqueidentifier] NULL,
	[bindata] [varbinary](max) NULL )
```

Для этой таблицы создается её отражение в виде C#-класса:

```
 [ObjectName("TestModel", "dbo")]
 public class TestModel
 {
     [IgnoreChanges, Unique, Autoincrement]
     public int Id { get; set; }
     public long? LongId { get; set; }
     public short? ShortId { get; set; }
     public string Name { get; set; }
     public string CharCode { get; set; }
     public bool Checked { get; set; }
     public int Value { get; set; }
     public float FValue { get; set; }
     public double GValue { get; set; }
     public decimal Money { get; set; }
     public DateTime Timestamp { get; set; }
     public TimeSpan Duration { get; set; }
     public Guid Guid { get; set; }
     public byte[] bindata { get; set; }
 }
```

Атрибут **ObjectName** уточняет реальное название объекта данных и схему (таблицы или представления). Атрибут не обязательный, но иногда лучше помочь СУБД, явно указав, в какой схеме искать таблицу.

Каждая колонка таблицы становится открытым свойством класса. Особое внимание уделяется полям, с помощью которых реализуется идентификация строк. Колонка Id, ставшая свойством, помечается атрибутами **IgnoreChanges**, **Unique**, **Autoincrement**. **IgnoreChanges** запрещает автогенератору CRUD-команд хоть как-то вносить или менять значения в этой колонке, т.к. она является вычисляемой. Атрибут **Autoincrement** по сути является такой же подсказкой, но полезен при разворачивании моделей данных с нуля. Атрибут **Unique** подчеркивает, что при генерации команд UPDATE и DELETE для фильтрации записей следует использовать эту колонку.

Существует множество других вспомогательных атрибутов для детального описания моделей и их свойств. Все они находятся в пространстве имен **DataTools.Attributes**.

#### Создание контекста работы с данными и простой запрос.

Строка подключения к СУБД, дополнительные функции преобразования сырых данных в типы полей (custom type converter) и в модели (custom model mapper) содержаться в объекте IDataContext.

Например, реализация контекста с СУБД MSSQL называется **MSSQL_DataContext**. Сначала создается его экземпляр и ему передается строка подключения:
```
var context = new MSSQL_DataContext();
context.ConnectionString = ...;
```
После этого запрос к таблице с данными можно осуществлять через функцию IDataContext.Select<T>:
```
IEnumerable<TestModel> result = context.Select<TestModel>();
```
Внутренние механизмы ORM возьмут из кеша заранее сгенерированную метамодель, на её основе сформируют команду SELECT, включающую все описыванные поля из класса TestModel. При запросе выполняется соединение с СУБД. После завершения обхода результата соединение с СУБД закрывается.

Для запроса данных с фильтрацией необходимо явно определить структуру запроса SELECT с помощью класса SqlSelect и передать экземпляр в IDataContext.Select.

```
// вариант 1
IEnumerable<TestModel> result = context.SelectFrom<TestModel>().Where(new SqlWhere().Name("имяКолонки").EqValue(значение)).Select();
// вариант 2
IEnumerable<TestModel> result = context.Select<TestModel>(new SqlSelect().From<TestModel>().Where(new SqlWhere().Name("имяКолонки").EqValue(значение)));
// вариант 3
IEnumerable<object[]> result = context.ExecuteWithResult(new SqlSelect().From<TestModel>().Where(new SqlWhere().Name("имяКолонки").EqValue(значение)));
// вариант 4
var mm = ModelMetadata<TestModel>.Instance;
IEnumerable<dynamic> result = context.SelectFrom(mm).Where(new SqlWhere().Name("имяКолонки").EqValue(значение)).Select();
// вариант 5
var mm = ModelMetadata<TestModel>.Instance;
IEnumerable<dynamic> result = context.Select(mm, new SqlSelect().From(mm).Where(new SqlWhere().Name("имяКолонки").EqValue(значение)));
```

В основном разнообразие возможностей формирования запроса данных обеспечивается благодаря классам расширения в пространстве имен **DataTools.Extensions**.
