using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace BigDataStore
{


    /// <summary>
    /// Indexes the low level pointers by category and by primary key
    /// </summary>
    public class Index: IDisposable
    {
        readonly Dictionary<string, Dictionary<string, Pointer>> _pointersByKeyByCategory = new Dictionary<string, Dictionary<string, Pointer>>();

        private readonly ReaderWriterLockSlim _sync = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        readonly FileStream _stream;

        public Index(string path)
        {
            const string fileName = "index.bin";

            var fullPath = Path.Combine(path, fileName);

            _stream = new FileStream(fullPath, FileMode.OpenOrCreate);

            InternalLoad();

        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public void Put(string category, string key, Pointer pointer)
        {

            try
            {
                _sync.TryEnterWriteLock(0);

                InternalSave(category, key, pointer);

                InternalPut(category, key, pointer);
            }
            finally
            {
                _sync.ExitWriteLock();
            }
            
        }

        private void InternalPut(string category, string key, Pointer pointer)
        {
            if (!_pointersByKeyByCategory.TryGetValue(category, out var byKey))
            {
                byKey = new Dictionary<string, Pointer>();
                _pointersByKeyByCategory[category] = byKey;
            }

            byKey[key] = pointer;
        }

        public Pointer TryGet(string category, string key)
        {
            try
            {
                _sync.TryEnterReadLock(0);

                if (_pointersByKeyByCategory.TryGetValue(category, out var byKey))
                {
                    if (byKey.TryGetValue(key, out var pointer))
                    {
                        return pointer;
                    }
                }

                return null;
            }
            finally
            {
                _sync.ExitReadLock();
            }
        }


        /// <summary>
        /// Append only save. When loading duplicate keys the last one is the good one
        /// </summary>
        /// <param name="category"></param>
        /// <param name="key"></param>
        /// <param name="pointer"></param>
        private void InternalSave(string category, string key, Pointer pointer)
        {
            
            using var w = new BinaryWriter(_stream);
            w.Write(category);
            w.Write(key);
            w.Write(pointer.FileIndex);
            w.Write(pointer.DocumentIndex);
            w.Flush();
        }

        private void InternalLoad()
        {
            _stream.Seek(0, SeekOrigin.End);

            var r = new BinaryReader(_stream);

            while (true)
            {
                var category = r.ReadString();
                var key = r.ReadString();

                var file = r.ReadInt32();
                var docInFile = r.ReadInt32();
                
                InternalPut(category, key, new Pointer(file, docInFile));

            }
        }

    }



    /// <summary>
    /// A pointer uniquely identifies an object in a low lever binary data store
    /// </summary>
    public class Pointer
    {
        public Pointer(int fileIndex, int documentIndex)
        {
            if (fileIndex < 0) throw new ArgumentException("File index should be zero or a positive integer");

            if (documentIndex < 0) throw new ArgumentException("Document index should be zero or a positive integer");

            FileIndex = fileIndex;

            DocumentIndex = documentIndex;
        }

        public int FileIndex { get; }

        public int DocumentIndex { get; }


        private bool Equals(Pointer other)
        {
            return FileIndex == other.FileIndex && DocumentIndex == other.DocumentIndex;
        }

        public override string ToString()
        {
            return $"{FileIndex}, {DocumentIndex}";
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Pointer) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = FileIndex;
                hashCode = (hashCode * 397) ^ DocumentIndex;
                return hashCode;
            }
        }
    }
}