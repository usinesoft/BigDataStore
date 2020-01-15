using System.IO;

namespace BigDataStore
{
    public static class StoreFactory
    {
        public static IBinaryDataStore CreateStore(StoreType storeType, string path, int binaryFileDataSize = Consts.DefaultBinaryFileDataSize, int maxObjectsInFile = Consts.DefaultMaxDocumentsInOneFile)
        {
            switch (storeType)
            {
                case StoreType.PlainFile:
                    return new PlainFile.DocumentStore(path, binaryFileDataSize, maxObjectsInFile);
                    
                case StoreType.MemoryMappedSafe:
                    return new MemoryMappedSafe.DocumentStore(path, binaryFileDataSize, maxObjectsInFile);

                case StoreType.MemoryMappedUnsafe:
                    return new MemoryMappedUnsafe.DocumentStore(path, binaryFileDataSize, maxObjectsInFile);

                default:
                    return new MemoryMappedUnsafe.DocumentStore(path, binaryFileDataSize, maxObjectsInFile);
            }
        }

        public static  void DeleteStore(string path)
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }
    }
}
