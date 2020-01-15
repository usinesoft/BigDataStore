using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BigDataStore.PlainFile
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

        private readonly List<FileStream> _files = new List<FileStream>();

        /// <summary>
        ///     Maximum number of documents that can be stored in a binary file (one million by default)
        /// </summary>
        private readonly int _maxDocuments;


        private FileStream _currentWriteView;


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
                    var stream = new FileStream(fileName, FileMode.Open);
                    _files.Add(stream);
                }

                if (_files.Count > 0) _currentWriteView = _files.Last();
            }
            else
            {
                Directory.CreateDirectory(StoragePath);
            }

            _fileMap = new List<int[]>(_files.Count);

            if (_files.Count == 0)
                CreateNewFile(1);
            else
                ReadMap();
        }


        /// <summary>
        ///     (Number of documents + 1) X size of an offset (int) + the size of an int for the document counter
        /// </summary>
        private int BinaryFileIndexSize => (_maxDocuments + 1) * sizeof(int) + sizeof(int);


        public string StoragePath { get; }

        #region Implementation of IDisposable

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
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
                    CreateNewFile(_files.Count + 1);
                }

                WriteBytes(_firstFreeOffset, documentData);

                _fileMap[_files.Count - 1][_documentsInCurrentView] = _firstFreeOffset;

                _documentsInCurrentView++;

                _firstFreeOffset += documentData.Length;

                _fileMap[_files.Count - 1][_documentsInCurrentView] = _firstFreeOffset;

                var offsetOfNextOffset = sizeof(int) + _documentsInCurrentView * sizeof(int);
                WriteInt(offsetOfNextOffset, _firstFreeOffset);

                WriteInt(0, _documentsInCurrentView);


                return new Pointer(_files.Count - 1, _documentsInCurrentView - 1);
            }
        }

        public byte[] LoadDocument(Pointer pointer)
        {
            lock (_syncRoot)
            {
                var view = _files[pointer.FileIndex];

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
                foreach (var view in _files)
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
                var newFile = new FileStream(path, FileMode.CreateNew);

                _currentWriteView = newFile;
                _files.Add(_currentWriteView);

                WriteInt(0, 0); // zero documents for now
                _documentsInCurrentView = 0;

                WriteInt(sizeof(int), BinaryFileIndexSize); // the first free offset is at the end of the index

                var offsets = new int[_maxDocuments + 1];
                _fileMap.Add(offsets);

                offsets[0] = _firstFreeOffset = BinaryFileIndexSize;
            }
        }


        private void ReadMap()
        {
            foreach (var fileStream in _files) ReadFileMap(fileStream);
        }

        private void ReadFileMap(FileStream fileStream)
        {
            lock (_syncRoot)
            {
                fileStream.Seek(0, SeekOrigin.Begin);

                var reader = new BinaryReader(fileStream);
                var count = reader.ReadInt32();

                var offsets = new int[_maxDocuments + 1];

                _fileMap.Add(offsets);

                for (var i = 0; i <= count; i++) offsets[i] = reader.ReadInt32();
            }
        }

        private void WriteInt(int offset, int value)
        {
            lock (_syncRoot)
            {
                _currentWriteView.Seek(offset, SeekOrigin.Begin);
                var writer = new BinaryWriter(_currentWriteView);

                writer.Write(value);
                writer.Flush();
            }
        }

        private byte[] ReadBytes(int offset, int num, FileStream view)
        {
            lock (_syncRoot)
            {
                view.Seek(offset, SeekOrigin.Begin);
                var reader = new BinaryReader(view);

                return reader.ReadBytes(num);
            }
        }

        private void WriteBytes(int offset, byte[] data)
        {
            lock (_syncRoot)
            {
                _currentWriteView.Seek(offset, SeekOrigin.Begin);
                var writer = new BinaryWriter(_currentWriteView);

                writer.Write(data);
                writer.Flush();
            }
        }
    }
}