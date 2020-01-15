namespace BigDataStore
{

    /// <summary>
    /// Generic interface for data storage
    /// Each object belongs to a category and has a unique (inside the category) primary key
    /// </summary>
    public interface IDataStore
    {
        /// <summary>
        /// Returns null if not found
        /// </summary>
        /// <param name="primaryKey"></param>
        /// <param name="category"></param>
        /// <returns></returns>
        byte[] TryGetObject(string primaryKey, string category = "");

        void PutObject(string primaryKey, byte[] data, string category = "");
        
        void DeleteObject(string primaryKey, string category = "");

    }
}