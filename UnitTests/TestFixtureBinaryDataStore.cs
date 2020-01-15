using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BigDataStore;
using NUnit.Framework;

namespace UnitTests
{

    [TestFixture]
    public class TestFixtureBinaryDataStore
    {
        private IBinaryDataStore _store;

        private string _storagePath = "store";

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            StoreFactory.DeleteStore(_storagePath);
        }


        
        [TearDown]
        public void TearDown()
        {
            _store.Dispose();
            StoreFactory.DeleteStore(_storagePath);
        }

        class StoredData
        {
            public Pointer Pointer { get; set; }
            public byte Fill { get; set; }
            public int Size { get; set; }
        }

        [Test]
        [TestCase(1000, StoreType.MemoryMappedUnsafe)]
        [TestCase(10_000, StoreType.MemoryMappedUnsafe)]
        [TestCase(1000, StoreType.MemoryMappedSafe)]
        [TestCase(10_000, StoreType.MemoryMappedSafe)]
        [TestCase(1000, StoreType.PlainFile)]
        [TestCase(10_000, StoreType.PlainFile)]
        public void Store_and_retrieve_objects(int count, StoreType storeType)
        {
            _store = StoreFactory.CreateStore(storeType, _storagePath);
            
            var stored = StoreData(count);

            CheckData(stored);



        }

        [Test]
        [TestCase(1000, StoreType.MemoryMappedUnsafe)]
        [TestCase(10_000, StoreType.MemoryMappedUnsafe)]
        [TestCase(1000, StoreType.MemoryMappedSafe)]
        [TestCase(10_000, StoreType.MemoryMappedSafe)]
        [TestCase(1000, StoreType.PlainFile)]
        [TestCase(10_000, StoreType.PlainFile)]
        public void Store_and_retrieve_objects_with_new_store_instance(int count, StoreType storeType)
        {
            _store = StoreFactory.CreateStore(storeType, _storagePath);

            var stored = StoreData(count);

            _store.Dispose();

            _store = StoreFactory.CreateStore(storeType, _storagePath);

            CheckData(stored);

        }

        [Test]
        [TestCase(1000, StoreType.MemoryMappedUnsafe)]
        [TestCase(10_000, StoreType.MemoryMappedUnsafe)]
        [TestCase(1000, StoreType.MemoryMappedSafe)]
        [TestCase(10_000, StoreType.MemoryMappedSafe)]
        [TestCase(1000, StoreType.PlainFile)]
        [TestCase(10_000, StoreType.PlainFile)]
        public void Read_all_documents(int count, StoreType storeType)
        {

            _store = StoreFactory.CreateStore(storeType, _storagePath);

            var stored = StoreData(count);

            var watch = new Stopwatch();

            watch.Start();
            int i = 0;
            foreach (var pair in _store.AllDocuments())
            {
                Assert.AreEqual(pair.Key, stored[i].Pointer);

                i++;
            }

            watch.Stop();

            Assert.AreEqual(count, i);

            Console.WriteLine($"Loading {count} documents took {watch.ElapsedMilliseconds} ms");
        }

        private void CheckData(List<StoredData> stored)
        {
            var watch = new Stopwatch();

            watch.Start();

            foreach (var storedData in stored)
            {
                var data = _store.LoadDocument(storedData.Pointer);
                Assert.AreEqual(data.Length, storedData.Size);
                Assert.IsTrue(data.All(b => b == storedData.Fill));
            }

            Console.WriteLine($"Loading {stored.Count} objects took {watch.ElapsedMilliseconds} ms ");
        }

        private List<StoredData> StoreData(int count)
        {
            var watch = new Stopwatch();

            watch.Start();
            var stored = new List<StoredData>();

            for (int i = 0; i < count; i++)
            {
                var random = new Random(Environment.TickCount);
                var size = random.Next(1_000_000);
                var fill = (byte) random.Next(256);

                var data = Enumerable.Repeat(fill, size).ToArray();

                var pointer = _store.StoreNewDocument(data);

                stored.Add(new StoredData {Fill = fill, Size = size, Pointer = pointer});
            }

            watch.Stop();

            Console.WriteLine($"Storing {count} objects took {watch.ElapsedMilliseconds} ms ");

            return stored;
        }



        [Test]
        [TestCase(StoreType.MemoryMappedUnsafe)]
        [TestCase(StoreType.MemoryMappedSafe)]
        [TestCase(StoreType.PlainFile)]
        public void Simple_operations(StoreType storeType)
        {
            _store = StoreFactory.CreateStore(storeType, _storagePath,  2000, 1) ;

            var data1 = Enumerable.Repeat((byte)13, 1500).ToArray();
            var p1 = _store.StoreNewDocument(data1);

            var data2 = Enumerable.Repeat((byte)17, 100).ToArray();
            var p2 = _store.StoreNewDocument(data2);

            var d1 = _store.LoadDocument(p1);
            Assert.AreEqual(1500, d1.Length);
            Assert.IsTrue(d1.All(d=>d==13));

            var d2 = _store.LoadDocument(p2);
            Assert.AreEqual(100, d2.Length);
            Assert.IsTrue(d2.All(d=>d==17));

            _store.Dispose();

            _store = StoreFactory.CreateStore(StoreType.PlainFile, _storagePath);

            d1 = _store.LoadDocument(p1);
            Assert.AreEqual(1500, d1.Length);
            Assert.IsTrue(d1.All(d=>d==13));

            d2 = _store.LoadDocument(p2);
            Assert.AreEqual(100, d2.Length);
            Assert.IsTrue(d2.All(d=>d==17));
        }
    }
}
