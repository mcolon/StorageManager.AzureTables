using Microsoft.WindowsAzure.Storage.Table;

namespace StorageManager.AzureTables
{
    public class TableStorageContinuationInfo
    {
        public TableContinuationToken ContinuationToken { get; set; }
        public TableQuery Query { get; set; }
        public int PageSize { get; set; }
        public bool HasMoreResult { get; set; }

    }
}