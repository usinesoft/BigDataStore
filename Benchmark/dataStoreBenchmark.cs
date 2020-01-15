using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using BigDataStore;
using BigDataStore.MemoryMappedUnsafe;
using Pointer = BigDataStore.Pointer;

namespace Benchmark
{

    [SimpleJob]
    [RPlotExporter, RankColumn]
    public class DataStoreBenchmark
    {
        private IBinaryDataStore _store;
        private byte[] _dataSmall;
        private byte[] _dataMedium;
        private byte[] _dataBig;
        private byte[] _dataHuge;

        private readonly List<Pointer> _pointers = new List<Pointer>();

        //string _storagePath = "temp";
        

        [GlobalSetup]
        public void GlobalSetup()
        {

            _store = StoreFactory.CreateStore(StoreType, Guid.NewGuid().ToString());

            _dataSmall = new byte[150];
            _dataMedium = new byte[1500];
            _dataBig = new byte[15000];
            _dataHuge = new byte[150_000];

            for (int i = 0; i < TotalObjectsCount; i++)
            {
                _pointers.Add(_store.StoreNewDocument(_dataSmall));
                _pointers.Add(_store.StoreNewDocument(_dataMedium));
                _pointers.Add(_store.StoreNewDocument(_dataBig));
                _pointers.Add(_store.StoreNewDocument(_dataHuge));
            }

        }

        [GlobalCleanup]
        public void Setup()
        {
            _store.Dispose();
        }


        [Params(10_000, 100_000, 200_000)]
        public int TotalObjectsCount { get; set; }

        [Params(StoreType.PlainFile, StoreType.MemoryMappedSafe, StoreType.MemoryMappedUnsafe)]
        public StoreType StoreType { get; set; }

        
        [Benchmark]
        public void LoadRandom_1000()
        {
            var rand = new Random();

            for (int i = 0; i < 1000; i++)
            {
                var pt = rand.Next(_pointers.Count);

                var _ = _store.LoadDocument(_pointers[pt]);
            }
        }

        [Benchmark] public void LoadSmall_1000()
        {
            var rand = new Random();

            for (int i = 0; i < 1000; i++)
            {
                var pt = rand.Next(_pointers.Count);

                pt -= (pt % 4);

                var _ = _store.LoadDocument(_pointers[pt]);
            }
        }

        [Benchmark] public void LoadMedium_1000()
        {
            var rand = new Random();

            for (int i = 0; i < 1000; i++)
            {
                var pt = rand.Next(_pointers.Count);

                pt = pt - (pt % 4) + 1;

                var _ = _store.LoadDocument(_pointers[pt]);
            }
        }

        [Benchmark] public void LoadBig_1000()
        {
            var rand = new Random();

            for (int i = 0; i < 1000; i++)
            {
                var pt = rand.Next(_pointers.Count);

                pt = pt - (pt % 4) + 2;

                var _ = _store.LoadDocument(_pointers[pt]);
            }
        }

        [Benchmark] public void LoadHuge_1000()
        {
            var rand = new Random();

            for (int i = 0; i < 1000; i++)
            {
                var pt = rand.Next(_pointers.Count);

                pt = pt - (pt % 4) + 3;

                var _ = _store.LoadDocument(_pointers[pt]);
            }
        }

        
    }

    
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<DataStoreBenchmark>();
        }
    }
}
