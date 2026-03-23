using DataTools.DML;
using DataTools.Extensions;
using DataTools.Interfaces;
using DataTools.Meta;
using System;
using System.Collections.Generic;

namespace DataTools_Tests
{
    public class TestData
    {
        public TestModel[] testModels;
        public TestModelExtra[] testModelExtras;
        public TestModelChild[] testModelChilds;
        public TestModelGuidParent[] testModelGuidParents;
        public TestModelGuidChild[] testModelGuidChilds;
        public TestModelSimple[] testModelSimples;
        public TestModelParentCompositePrimaryKey[] testModelParentCompositePrimaryKeys;
        public TestModelChildCompositePrimaryKey[] testModelChildCompositePrimaryKeys;
        public TestModelPrimaryKeyAsForeignKey[] testModelPrimaryKeyAsForeignKeys;
        public TestModelCompositePrimaryKey[] testModelCompositePrimaryKeys;

        public IEnumerable<IModelMetadata> Metadatas = new List<IModelMetadata> {
            ModelMetadata<TestModel>.Instance,
            ModelMetadata<TestModelExtra>.Instance,
            ModelMetadata<TestModelChild>.Instance,
            ModelMetadata<TestModelGuidParent>.Instance,
            ModelMetadata<TestModelGuidChild>.Instance,
            ModelMetadata<TestModelSimple>.Instance,
            ModelMetadata<TestModelParentCompositePrimaryKey>.Instance,
            ModelMetadata<TestModelChildCompositePrimaryKey>.Instance,
            ModelMetadata<TestModelPrimaryKeyAsForeignKey>.Instance,
            ModelMetadata<TestModelCompositePrimaryKey>.Instance
            };

        public TestData()
        {
            testModels = new TestModel[] {
                new TestModel() {
                    Id =1,
                    LongId =1L,
                    ShortId = 1,
                    Name = "TestModel1",
                    CharCode ="a",
                    Checked = false,
                    Value = 1,
                    FValue =1.1F,
                    GValue = 1.2,
                    Money = (decimal)1.3,
                    Timestamp = DateTime.Parse("2024-01-01"),
                    Duration = TimeSpan.Parse("23:59:59"),
                    Guid = Guid.NewGuid(),
                    bindata = new byte[]{1,2,3,4 }
                },
                new TestModel() {
                    Id =2,
                    LongId =2L,
                    ShortId = 2,
                    Name = "TestModel2",
                    CharCode ="b",
                    Checked = true,
                    Value = 1,
                    FValue =1.1F,
                    GValue = 1.2,
                    Money = (decimal)1.3,
                    Timestamp = DateTime.Parse("2024-03-01"),
                    Duration = TimeSpan.Parse("01:01:01"),
                    Guid = Guid.NewGuid(),
                    bindata = new byte[]{255,255,255,255 }
                },
                new TestModel() {
                    Id =3,
                    LongId =null,
                    ShortId = null,
                    Name = "TestModel3",
                    CharCode ="b",
                    Checked = true,
                    Value = 1,
                    FValue =1.1F,
                    GValue = 1.3,
                    Money = (decimal)1.4,
                    Timestamp = DateTime.Parse("2024-04-01"),
                    Duration = TimeSpan.Parse("01:01:01"),
                    Guid = Guid.NewGuid(),
                    bindata = new byte[]{0,0,255,0 }
                }
            };

            testModelExtras = new TestModelExtra[] {
                new TestModelExtra() {
                    Id = 1,
                    Name = "TestModelExtra1",
                    Value = 1,
                    FValue = 1.1F,
                    GValue = 1.2,
                    Timestamp = DateTime.Parse("2024-01-01"),
                    Extra = null
                },
                new TestModelExtra() {
                    Id = 2,
                    Name = "TestModelExtra2",
                    Value = 1,
                    FValue = 1.1F,
                    GValue = 1.2,
                    Timestamp = DateTime.Parse("2023-01-01"),
                    Extra = null
                }
            };
            testModelExtras[0].Extra = testModelExtras[0];
            testModelExtras[1].Extra = testModelExtras[1];

            testModelChilds = new TestModelChild[]
            {
                new TestModelChild()
                {
                    Id = 1,
                    Name = "TestModelChild1",
                    Value = 1,
                    FValue = 1.1f,
                    GValue = 1.2,
                    Timestamp= DateTime.Parse("2024-01-01"),
                    Parent = testModels[0],
                    Extra = testModelExtras[0]
                },
                new TestModelChild()
                {
                    Id = 2,
                    Name = "TestModelChild2",
                    Value = 1,
                    FValue = 1.15f,
                    GValue = 1.25,
                    Timestamp= DateTime.Parse("2023-01-01"),
                    Parent = testModels[2],
                    Extra = testModelExtras[1]
                },
            };

            testModelGuidParents = new TestModelGuidParent[]
            {
                new TestModelGuidParent()
                {
                    Id = Guid.NewGuid(),
                    Name = "parent1"
                },
                new TestModelGuidParent()
                {
                    Id = Guid.NewGuid(),
                    Name = "parent2"
                }
           };

            testModelGuidChilds = new TestModelGuidChild[]
            {
                new TestModelGuidChild()
                {
                    Id = Guid.NewGuid(),
                    Name = "child1",
                    Parent = testModelGuidParents[0]
                },
                new TestModelGuidChild()
                {
                    Id = Guid.NewGuid(),
                    Name = "child2",
                    Parent = testModelGuidParents[1]
                }
            };

            testModelSimples = new TestModelSimple[]
          {
                new TestModelSimple()
                {
                    Id = 1,
                    Name = "a"
                },
                new TestModelSimple()
                {
                    Id = 2,
                    Name = "b"
                },
                new TestModelSimple()
                {
                    Id = 1,
                    Name = "c"
                },
                new TestModelSimple()
                {
                    Id = 3,
                    Name = "b"
                },
                new TestModelSimple()
                {
                    Id = null,
                    Name = "d"
                }
          };

            testModelParentCompositePrimaryKeys = new TestModelParentCompositePrimaryKey[]
        {
                new TestModelParentCompositePrimaryKey()
                {
                    i = 1,
                    j = 1,
                    k = "abc"
                },
                new TestModelParentCompositePrimaryKey()
                {
                    i = 1,
                    j = 2,
                    k = "def"
                },
                new TestModelParentCompositePrimaryKey()
                {
                    i = 2,
                    j = 2,
                    k = "ghi"
                },
                new TestModelParentCompositePrimaryKey()
                {
                    i = 3,
                    j = 3,
                    k = "jkl"
                },
        };

            testModelChildCompositePrimaryKeys = new TestModelChildCompositePrimaryKey[] {
                new TestModelChildCompositePrimaryKey()
                {
                    i = 1,
                    j = 1,
                    Parent = testModelParentCompositePrimaryKeys[0],
                    k = "abc"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 2,
                    j = 2,
                    Parent = testModelParentCompositePrimaryKeys[0],
                    k = "def"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 1,
                    j = 2,
                    Parent = testModelParentCompositePrimaryKeys[1],
                    k = "ghi"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 2,
                    j = 3,
                    Parent = testModelParentCompositePrimaryKeys[2],
                    k = "jkl"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 3,
                    j = 3,
                    Parent = testModelParentCompositePrimaryKeys[3],
                    k = "mno"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 4,
                    j = 1,
                    Parent = null,
                    k = "pqr"
                },
                new TestModelChildCompositePrimaryKey()
                {
                    i = 5,
                    j = 5,
                    Parent = testModelParentCompositePrimaryKeys[1],
                    k = "stu"
                }
            };

            testModelPrimaryKeyAsForeignKeys = new TestModelPrimaryKeyAsForeignKey[]
            {
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[0],
                    k = "abc"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[1],
                    k = "def"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[2],
                    k = "ghi"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[3],
                    k = "jkl"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[4],
                    k = "mno"
                },
                new TestModelPrimaryKeyAsForeignKey()
                {
                    Child = testModelChildCompositePrimaryKeys[5],
                    k = "pqr"
                }
            };

            testModelCompositePrimaryKeys = new TestModelCompositePrimaryKey[]
{
                new TestModelCompositePrimaryKey()
                {
                    i = 1,
                    j = 1,
                    k = "abc"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 2,
                    j = 2,
                    k = "def"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 1,
                    j = 2,
                    k = "ghi"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 2,
                    j = 3,
                    k = "jkl"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 3,
                    j = 3,
                    k = "mno"
                },
                new TestModelCompositePrimaryKey()
                {
                    i = 4,
                    j = 1,
                    k = "pqr"
                }
};
        }

        public void InsertTestData(IDataContext DataContext)
        {
            var testdata = new TestData();

            void ProcessInsertion<ModelT>(ModelT[] models) where ModelT : class, new()
            {
                var q = new SqlComposition();
                foreach (var m in models)
                    q.Add(new SqlInsert().Into<ModelT>().Value(m));
                DataContext.Execute(q);
            }
         ;

            ProcessInsertion(testdata.testModels);

            ProcessInsertion(testdata.testModelExtras);

            ProcessInsertion(testdata.testModelChilds);

            ProcessInsertion(testdata.testModelGuidParents);

            ProcessInsertion(testdata.testModelGuidChilds);

            ProcessInsertion(testdata.testModelSimples);

            ProcessInsertion(testdata.testModelParentCompositePrimaryKeys);

            ProcessInsertion(testdata.testModelChildCompositePrimaryKeys);

            ProcessInsertion(testdata.testModelPrimaryKeyAsForeignKeys);

            ProcessInsertion(testdata.testModelCompositePrimaryKeys);
        }
    }
}

