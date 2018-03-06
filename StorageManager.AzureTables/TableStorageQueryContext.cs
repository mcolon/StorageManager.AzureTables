using StorageManager.Helpers;

namespace StorageManager.AzureTables
{
    public class TableStorageQueryContext
    {
        public TableStorageContinuationInfo ContinuationInfo { get; set; }
        public bool HasMoreResult => ContinuationInfo?.HasMoreResult ?? true;
        public int PageSize { get; set; }

        public static TableStorageQueryContext Create(string serializedContext, int? pageSize = null)
        {
            var result =  BSonConvert.DeserializeFromBase64String<TableStorageQueryContext>(serializedContext);
            if (pageSize.HasValue)
            {
                result.ContinuationInfo.PageSize = pageSize.Value;
            }
            return result;
        }
    }
}