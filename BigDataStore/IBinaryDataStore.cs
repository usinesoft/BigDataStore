using System;
using System.Collections.Generic;

namespace BigDataStore
{
    public interface IBinaryDataStore: IDisposable
    {
        Pointer StoreNewDocument(byte[] documentData);
        byte[] LoadDocument(Pointer pointer);

        /// <summary>
        ///     Iterate on all the documents in all the files
        /// </summary>
        /// <returns></returns>
        IEnumerable<KeyValuePair<Pointer, byte[]>> AllDocuments();
    }
}