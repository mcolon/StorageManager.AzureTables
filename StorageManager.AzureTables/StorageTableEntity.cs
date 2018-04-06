using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace StorageManager.AzureTables
{
    public class StorageTableEntity : ITableEntity
    {
        public StorageTableEntity()
        {
            Properties = new Dictionary<string, EntityProperty>();
        }

        public StorageTableEntity(string partitionKey, string rowKey)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, null /* timestamp */, new Dictionary<string, EntityProperty>())
        {
        }

        public StorageTableEntity(string partitionKey, string rowKey, string etag, IDictionary<string, EntityProperty> properties)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, etag, properties)
        {
        }

        internal StorageTableEntity(string partitionKey, string rowKey, DateTimeOffset timestamp, string etag, IDictionary<string, EntityProperty> properties)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            Timestamp = timestamp;
            ETag = etag;

            Properties = properties;
        }

        public IDictionary<string, EntityProperty> Properties { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Properties = properties;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return Properties;
        }
    }
}