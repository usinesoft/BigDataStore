using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;

namespace BigDataStore.MemoryMappedUnsafe
{
    /// <summary>
    ///     Compiled documents are stored in fixed size binary files inside a directory
    ///     Files are named 001.bin 002.bin and are accessed using memory mapped files
    ///     FileLayout:
    ///     NumberOfContainedDocuments  : int32
    ///     DocumentOffsets             : (NumberOfContainedDocuments + 1) x int32
    ///     Documents                   : list of variable size binary documents
    /// </summary>
    public class DocumentStore : IBinaryDataStore
    {
        private readonly List<int[]> _fileMap;

        private readonly object _syncRoot = new object();

        /// <summary>
        ///     Total size of document data (one gigabyte by default)
        /// </summary>
        private readonly int _binaryFileDataSize;

        private readonly List<MemoryMappedFile> _files = new List<MemoryMappedFile>();

        /// <summary>
        ///     Maximum number of documents that can be stored in a binary file (one million by default)
        /// </summary>
        private readonly int _maxDocuments;

        private readonly List<MemoryMappedViewAccessor> _views = new List<MemoryMappedViewAccessor>();

        private MemoryMappedViewAccessor _currentWriteView;

        private int _documentsInCurrentView;
        private int _firstFreeOffset;

        public DocumentStore(string storagePath, int binaryFileDataSize = Consts.DefaultBinaryFileDataSize,
            int maxDocumentsInEachFile = Consts.DefaultMaxDocumentsInOneFile)
        {
            _binaryFileDataSize = binaryFileDataSize;

            _maxDocuments = maxDocumentsInEachFile;


            StoragePath = storagePath;

            if (Directory.Exists(StoragePath))
            {
                var files = Directory.EnumerateFiles(StoragePath, Consts.BinaryFilePattern)
                    .OrderBy(name => name);

                foreach (var fileName in files)
                {
                    var mmFile = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open);
                    _views.Add(mmFile.CreateViewAccessor());
                    _files.Add(mmFile);
                }

                if (_views.Count > 0) _currentWriteView = _views.Last();
            }
            else
            {
                Directory.CreateDirectory(StoragePath);
            }

            _fileMap = new List<int[]>(_files.Count);

            if (_views.Count == 0) CreateNewFile(1);


            ReadMap();
        }


        /// <summary>
        ///     (Number of documents + 1) X size of an offset (int) + the size of an int for the document counter
        /// </summary>
        private int BinaryFileIndexSize => (_maxDocuments + 1) * sizeof(int) + sizeof(int);

        private int BinaryFileSize => _binaryFileDataSize + BinaryFileIndexSize;

        public string StoragePath { get; }

        #region Implementation of IDisposable

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            foreach (var view in _views) view.Dispose();

            foreach (var file in _files) file.Dispose();
        }

        #endregion


        public Pointer StoreNewDocument(byte[] documentData)
        {
            lock (_syncRoot)
            {
                if (documentData.Length > _binaryFileDataSize)
                    throw new NotSupportedException("Document size exceeds binary file size");

                if (_documentsInCurrentView < _maxDocuments &&
                    _firstFreeOffset + documentData.Length < _binaryFileDataSize)
                {
                    var offsetOfOffset = sizeof(int) + _documentsInCurrentView * sizeof(int);

                    WriteInt(offsetOfOffset, _firstFreeOffset);
                }
                else // create a new file
                {
                    CreateNewFile(_views.Count + 1);
                }

                WriteBytes(_firstFreeOffset, documentData);

                _fileMap[_views.Count - 1][_documentsInCurrentView] = _firstFreeOffset;

                _documentsInCurrentView++;
                _firstFreeOffset += documentData.Length;

                _fileMap[_views.Count - 1][_documentsInCurrentView] = _firstFreeOffset;

                var offsetOfNextOffset = sizeof(int) + _documentsInCurrentView * sizeof(int);
                WriteInt(offsetOfNextOffset, _firstFreeOffset);

                WriteInt(0, _documentsInCurrentView);


                return new Pointer(_views.Count - 1, _documentsInCurrentView - 1);
            }
        }

        public byte[] LoadDocument(Pointer pointer)
        {
            lock (_syncRoot)
            {
                var view = _views[pointer.FileIndex];

                var offset = _fileMap[pointer.FileIndex][pointer.DocumentIndex];

                var nextOffset = _fileMap[pointer.FileIndex][pointer.DocumentIndex + 1];

                return ReadBytes(offset, nextOffset - offset, view);
            }
        }

        /// <summary>
        ///     Iterate on all the documents in all the files
        /// </summary>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<Pointer, byte[]>> AllDocuments()
        {
            lock (_syncRoot)
            {
                var fileIndex = 0;
                foreach (var view in _views)
                {
                    var offsets = _fileMap[fileIndex];

                    for (var docIndex = 0; docIndex < _maxDocuments; docIndex++)
                    {
                        var offset = offsets[docIndex];

                        var nextOffset = offsets[docIndex + 1];
                        if (nextOffset == 0) break;

                        var data = ReadBytes(offset, nextOffset - offset, view);

                        yield return new KeyValuePair<Pointer, byte[]>(new Pointer(fileIndex, docIndex), data);
                    }

                    fileIndex++;
                }
            }
        }

        private void CreateNewFile(int index)
        {
            lock (_syncRoot)
            {
                var extension = Consts.BinaryFilePattern.Trim('*');
                var fileName = index.ToString("D4") + extension;

                var path = Path.Combine(StoragePath, fileName);
                var newFile = MemoryMappedFile.CreateFromFile(path, FileMode.CreateNew, fileName, BinaryFileSize);

                _currentWriteView = newFile.CreateViewAccessor();
                _views.Add(_currentWriteView);
                _files.Add(newFile);

                WriteInt(0, 0); // zero documents for now
                _documentsInCurrentView = 0;

                WriteInt(sizeof(int), BinaryFileIndexSize); // the first free offset is at the end of the index
                _firstFreeOffset = BinaryFileIndexSize;

                var offsets = new int[_maxDocuments + 1];
                _fileMap.Add(offsets);

                offsets[0] = _firstFreeOffset = BinaryFileIndexSize;
            }
        }


        private void ReadMap()
        {
            foreach (var viewAccessor in _views) ReadFileMap(viewAccessor);
        }

        private unsafe void ReadFileMap(MemoryMappedViewAccessor view)
        {
            lock (_syncRoot)
            {
                var buffer = new byte[BinaryFileIndexSize];
                var ptr = (byte*) 0;

                try
                {
                    view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                    Marshal.Copy(new IntPtr(ptr), buffer, 0, BinaryFileIndexSize);

                    fixed (byte* bufferPtr = buffer)
                    {
                        var intPtr = (int*) bufferPtr;
                        var count = *intPtr;

                        var offsets = new int[_maxDocuments + 1];
                        _fileMap.Add(offsets);
                        for (var i = 0; i <= count; i++)
                        {
                            intPtr++;

                            offsets[i] = *intPtr;
                        }
                    }
                }
                finally
                {
                    view.SafeMemoryMappedViewHandle.ReleasePointer();
                }
            }
        }

        private unsafe void WriteInt(int offset, int value)
        {
            lock (_syncRoot)
            {
                var buffer = new byte[4];

                new BinaryWriter(new MemoryStream(buffer)).Write(value);

                var ptr = (byte*) 0;

                try
                {
                    _currentWriteView.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                    Marshal.Copy(buffer, 0, IntPtr.Add(new IntPtr(ptr), offset), buffer.Length);
                }
                finally
                {
                    _currentWriteView.SafeMemoryMappedViewHandle.ReleasePointer();
                }
            }
        }

        private unsafe byte[] ReadBytes(int offset, int num, MemoryMappedViewAccessor view)
        {
            lock (_syncRoot)
            {
                var arr = new byte[num];
                var ptr = (byte*) 0;
                view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                Marshal.Copy(IntPtr.Add(new IntPtr(ptr), offset), arr, 0, num);
                view.SafeMemoryMappedViewHandle.ReleasePointer();
                return arr;
            }
        }

        private unsafe void WriteBytes(int offset, byte[] data)
        {
            lock (_syncRoot)
            {
                var ptr = (byte*) 0;
                _currentWriteView.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                Marshal.Copy(data, 0, IntPtr.Add(new IntPtr(ptr), offset), data.Length);
                _currentWriteView.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
    }
}