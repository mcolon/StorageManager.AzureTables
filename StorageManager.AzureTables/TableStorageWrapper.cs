using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using StorageManager.Configuration;
using StorageManager.Exceptions;
using StorageManager.Helpers;
using StorageManager.Query;
using StorageManager.Storage;

namespace StorageManager.AzureTables
{
    public class TableStorageWrapper<T> : StorageWrapper<T>
    {
        const string SCHEMA_CONSTANT_STRING = "SCHEMA_DEFINITION_HASH";
        private const int TimeoutForStorageResource = 5000;

        static readonly AutoResetEvent _creatingTable = new AutoResetEvent(true);

        private CloudTable _table;

        public TableStorageWrapper(StorageEntityDefinition<T> definition, IStorageConfiguration configuration) : base(definition, configuration)
        {
        }



        public override async Task Insert(T entity)
        {
            var operations = BuildTableOperationInsert(entity);

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }

        public override async Task InsertOrUpdate(T entity)
        {
            var operations = await BuildTableOperationUpsert(entity).ConfigureAwait(false);

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }

        public override async Task Update(T entity)
        {
            var operations = await BuildTableOperationUpdate(entity).ConfigureAwait(false);

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }

        public override async Task Delete(T entity)
        {
            var operations = await BuildTableOperationDelete(entity).ConfigureAwait(false);

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }



        public override async Task Insert(IEnumerable<T> entity)
        {
            var operations = entity.SelectMany(BuildTableOperationInsert).ToList();

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }

        public override async Task InsertOrUpdate(IEnumerable<T> entities)
        {
            List<TableOperation> operations = new List<TableOperation>();

            foreach (var entity in entities)
            {
                var entityOperations = await BuildTableOperationUpsert(entity).ConfigureAwait(false);
                operations.AddRange(entityOperations);
            }

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }

        public override async Task Update(IEnumerable<T> entities)
        {
            List<TableOperation> operations = new List<TableOperation>();

            foreach (var entity in entities)
            {
                var entityOperations = await BuildTableOperationUpdate(entity).ConfigureAwait(false);
                operations.AddRange(entityOperations);
            }

            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }

        public override async Task Delete(IEnumerable<T> entities)
        {
            List<TableOperation> operations = new List<TableOperation>();

            foreach (var entity in entities)
            {
                var entityOperations = await BuildTableOperationDelete(entity).ConfigureAwait(false);
                operations.AddRange(entityOperations);
            }
            await ExecuteBatchOperations(operations).ConfigureAwait(false);
        }



        private List<TableOperation> BuildTableOperationInsert(T entity)
        {
            List<KeyValuePair<string, EntityProperty>> fields = GetEntityFilterableFields(entity).ToList();

            EntityProperty serializedField = EntityProperty.GeneratePropertyForByteArray(BSonConvert.SerializeObject(entity));

            var record = GenerateRecordMainPartition(entity, serializedField, fields);
            List<TableOperation> operations = new List<TableOperation>
            {
                TableOperation.Insert(record)
            };

            foreach (var partitionRecord in GeneratePersistPartitionData(entity, serializedField, fields))
                operations.Add(TableOperation.Insert(partitionRecord));

            return operations;
        }

        private async Task<List<TableOperation>> BuildTableOperationUpsert(T entity)
        {
            var entityId = EntityDefinition.GetIdValues(entity);
            var serializedField = EntityProperty.GeneratePropertyForByteArray(BSonConvert.SerializeObject(entity));
            var fields = GetEntityFilterableFields(entity).ToList();
            var record = GenerateRecordMainPartition(entity, serializedField, fields);

            List<TableOperation> operations = new List<TableOperation>
            {
                TableOperation.InsertOrReplace(record)
            };

            operations.AddRange(GeneratePersistPartitionData(entity, serializedField, fields)
                .Select(TableOperation.InsertOrReplace));
            var idValue = string.Join(TableStorageQueryBuilder.PARTITION_FIELD_SEPARATOR,
                entityId.Select(TableStorageQueryBuilder.NormalizeStringValue));

            var old = await GetById(TableStorageQueryBuilder.GetTableKeyNormalizedValue(idValue)).ConfigureAwait(false);
            if (old != null)
            {
                var oldFields = GetEntityFilterableFields(old);
                IEnumerable<TableOperation> deleteOperations = GeneratePersistPartitionData(old, null, oldFields)
                    .Select(TableOperation.Delete)
                    .Where(d => !operations.Any(o =>
                        o.Entity.PartitionKey == d.Entity.PartitionKey && o.Entity.RowKey == d.Entity.RowKey))
                    .ToList();
                operations.AddRange(deleteOperations);
            }
            return operations;
        }

        private async Task<List<TableOperation>> BuildTableOperationUpdate(T entity)
        {
            var entityIdValues = EntityDefinition.GetIdValues(entity);
            var idString = string.Join(TableStorageQueryBuilder.PARTITION_FIELD_SEPARATOR,
                entityIdValues.Select(TableStorageQueryBuilder.NormalizeStringValue));

            var serializedField = EntityProperty.GeneratePropertyForByteArray(BSonConvert.SerializeObject(entity));

            var fields = GetEntityFilterableFields(entity);

            var record = GenerateRecordMainPartition(entity, serializedField, fields);

            List<TableOperation> operations = new List<TableOperation>
            {
                TableOperation.Replace(record)
            };

            var old = await GetById(idString).ConfigureAwait(false);
            if (old != null)
            {
                var oldFields = GetEntityFilterableFields(old);
                operations.AddRange(GeneratePersistPartitionData(old, null, oldFields).Select(TableOperation.Delete));
            }
            return operations;
        }

        private async Task<List<TableOperation>> BuildTableOperationDelete(T entity)
        {
            var entityIdValues = EntityDefinition.GetIdValues(entity);
            var idString = string.Join(TableStorageQueryBuilder.PARTITION_FIELD_SEPARATOR,
                entityIdValues.Select(TableStorageQueryBuilder.NormalizeStringValue));

            var record = GenerateRecordMainPartition(entity, null, null);

            List<TableOperation> operations = new List<TableOperation>
            {
                TableOperation.Delete(record)
            };

            var old = await GetById(idString).ConfigureAwait(false);
            if (old != null)
            {
                var oldFields = GetEntityFilterableFields(old);
                operations.AddRange(GeneratePersistPartitionData(old, null, oldFields).Select(TableOperation.Delete));
            }
            return operations;
        }



        private async Task<T> GetById(string entityId)
        {
            var translator = new TableStorageQueryTranslator<T>(EntityDefinition);

            var query = translator.QueryGetById(entityId);
            var result = (await ExecuteQueryAsync(query).ConfigureAwait(false)).Records.FirstOrDefault();
            return result;
        }



        private IEnumerable<DynamicTableEntity> GeneratePersistPartitionData(T entity, EntityProperty serializedField, IEnumerable<KeyValuePair<string, EntityProperty>> fields)
        {
            List<DynamicTableEntity> result = new List<DynamicTableEntity>();

            foreach (var partition in GetPartitionsValues(entity))
            {
                var entityIdValues = EntityDefinition.GetIdValues(entity);
                var idString = string.Join(TableStorageQueryBuilder.PARTITION_FIELD_SEPARATOR, entityIdValues.Select(TableStorageQueryBuilder.NormalizeStringValue));

                var partitionKey = TableStorageQueryBuilder.GetPartitionKeyValue(partition.Key, partition.Value);
                partitionKey = string.Join(TableStorageQueryBuilder.PARTITION_NAME_SEPARATOR, partition.Key, partitionKey);

                DynamicTableEntity record = new DynamicTableEntity(partitionKey,
                    TableStorageQueryBuilder.GetTableKeyNormalizedValue(idString)) {ETag = "*"};
                foreach (var field in fields)
                {
                    if (field.Value != null)
                        record.Properties.Add(field);
                }
                record.Properties.Add("Content", serializedField);
                result.Add(record);
            }

            return result;
        }

        private DynamicTableEntity GenerateRecordMainPartition(T entity, EntityProperty serializedField, IEnumerable<KeyValuePair<string, EntityProperty>> fields)
        {
            var entityIdValues = EntityDefinition.GetIdValues(entity);
            var idString = string.Join(TableStorageQueryBuilder.PARTITION_FIELD_SEPARATOR, entityIdValues.Select(TableStorageQueryBuilder.NormalizeStringValue));

            DynamicTableEntity record = new DynamicTableEntity(TableStorageQueryBuilder.MAIN_PARTITION_NAME, TableStorageQueryBuilder.GetTableKeyNormalizedValue(idString)) {ETag = "*"};

            if (fields != null)
            {
                foreach (var field in fields)
                {
                    if (field.Value != null)
                        record.Properties.Add(field);
                }
            }

            record.Properties.Add("Content", serializedField);
            return record;
        }

        private IEnumerable<KeyValuePair<string, EntityProperty>>  GetEntityFilterableFields(T entity)
        {
            return EntityDefinition
                .Filters()
                .Select(k => new KeyValuePair<string, EntityProperty>(k.Name, GetAzureTableProperty(k.Accesor.Evaluate(entity))));
        }

        private EntityProperty GetAzureTableProperty(object value)
        {
            if (value == null)
                return null;

            EntityProperty result = null;
            var valueType = value.GetType();
            if (typeof(long) == valueType) result = new EntityProperty(value == null ? (long?)null : Convert.ToInt64(value));
            else if (typeof(int) == valueType) result = new EntityProperty(value == null ? (int?)null : Convert.ToInt32(value));
            else if (typeof(short) == valueType) result = new EntityProperty(value == null ? (short?)null : Convert.ToInt16(value));
            else if (typeof(byte) == valueType) result = new EntityProperty(value == null ? (byte?)null : Convert.ToByte(value));
            else if (typeof(float) == valueType) result = new EntityProperty(value == null ? (double?)null : Convert.ToDouble(value));
            else if (typeof(decimal) == valueType) result = new EntityProperty(value == null ? (double?)null : Convert.ToDouble(value));
            else if (typeof(double) == valueType) result = new EntityProperty(value == null ? (double?)null : Convert.ToDouble(value));
            else if (typeof(DateTime) == valueType) result = new EntityProperty(value == null ? (DateTime?)null : Convert.ToDateTime(value));
            else if (typeof(bool) == valueType) result = new EntityProperty(value == null ? (bool?)null : Convert.ToBoolean(value));
            else if (typeof(string) == valueType) result = new EntityProperty(value?.ToString());
            else if (valueType.IsEnum) result = new EntityProperty(value?.ToString());
            else
            {
                throw new StorageArgumentException($"Type {valueType.Name} not supported");
            }

            return result;
        }

        private byte[] ComputeDefinitionHash()
        {
            var bsonDefinition = BSonConvert.SerializeObject(EntityDefinition);

            using (var sha1Hash = SHA1.Create())
            {
                byte[] data = sha1Hash.ComputeHash(bsonDefinition);
                return data;
            }
        }

        private async Task<CloudTable> TableAsync()
        {
            if (_table == null)
            {
                if (_creatingTable.WaitOne(TimeoutForStorageResource))
                {
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Configuration.StorageAccount);
                    CloudTableClient client = storageAccount.CreateCloudTableClient();

                    var tbl = client.GetTableReference(EntityDefinition.TableName());
                    if (await tbl.ExistsAsync().ConfigureAwait(false))
                    {
                        if (! await CheckSchemaAsync(tbl))
                        {
                            if (!EntityDefinition.AutoRebuildPartitionIfRequire)
                                throw new StorageArgumentException("Schema table has changed or not exist");

                            await RebuildPartitionsAsync(tbl).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await tbl.CreateIfNotExistsAsync().ConfigureAwait(false);
                        await WriteSchemaAsync(tbl).ConfigureAwait(false);
                    }
                    _table = tbl;
                    _creatingTable.Set();
                }
                else
                    throw new TimeoutException($"Timeout waiting resource {EntityDefinition.TableName()}");
            }
            return _table;
        }

        private async Task RebuildPartitionsAsync(CloudTable tbl)
        {
            await Task.Delay(0).ConfigureAwait(false);
        }

        private void RebuildPartitions(CloudTable tbl)
        {
            
        }



        private async Task<bool> CheckSchemaAsync(CloudTable table)
        {
            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, SCHEMA_CONSTANT_STRING));
            var result = (await table.ExecuteQuerySegmentedAsync(query, null)).ToList();
            if (!result.Any())
                return false;

            var serialized = result.First().Properties["Content"].BinaryValue;
            var hashSchema = ComputeDefinitionHash();
            return serialized.SequenceEqual(hashSchema);
        }

        private async Task WriteSchemaAsync(CloudTable table)
        {
            await DeleteSchemaAsync(table).ConfigureAwait(false);

            DynamicTableEntity schema = new DynamicTableEntity(SCHEMA_CONSTANT_STRING, DateTime.UtcNow.ToString("s"));
            var hashData = ComputeDefinitionHash();
            var serializedField = EntityProperty.GeneratePropertyForByteArray(hashData);

            schema.Properties.Add("Content", serializedField);
            await table.ExecuteAsync(TableOperation.InsertOrReplace(schema)).ConfigureAwait(false);
        }

        private async Task DeleteSchemaAsync(CloudTable table)
        {
            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, SCHEMA_CONSTANT_STRING));
            var result = await table.ExecuteQuerySegmentedAsync(query, null);

            foreach (var schemadefinition in result)
            {
                await table.ExecuteAsync(TableOperation.Delete(schemadefinition)).ConfigureAwait(false);
            }
        }

        private async Task ExecuteBatchOperations(List<TableOperation> batchOperation)
        {
            var table = await TableAsync().ConfigureAwait(false);
            var operationGroups = batchOperation.GroupBy(b => b.Entity.PartitionKey).Select(b => b.AsEnumerable());
            List<Task> resultTasks = new List<Task>();
            foreach (var operations in operationGroups)
            {
                var listOperations = operations as TableOperation[] ?? operations.ToArray();
                int current = 0;
                do
                {
                    var batchSegment = listOperations.Skip(current).Take(90).ToList();
                    TableBatchOperation batchGroup = new TableBatchOperation();
                    foreach (var op in batchSegment)
                        batchGroup.Add(op);

                    resultTasks.Add(table.ExecuteBatchAsync(batchGroup));
                    current += batchSegment.Count();
                } while (current < listOperations.Count());
            }
            await Task.WhenAll(resultTasks).ConfigureAwait(false);
        }



        public override async Task<StorageQueryResult<T>> ExecuteQueryAsync(Expression expression)
        {
            var translator = new TableStorageQueryTranslator<T>(EntityDefinition);
            var toExecute = translator.Translate(expression);
            return await ExecuteQueryAsync(toExecute).ConfigureAwait(false);
        }

        public override async Task<StorageQueryResult<T>> ExecuteQueryAsync(string context, int? pageSize = null)
        {
            var queryContext = TableStorageQueryContext.Create(context, pageSize);
            return await ExecuteQueryAsync(queryContext).ConfigureAwait(false);
        }

        private async Task<StorageQueryResult<T>> ExecuteQueryAsync(TableQuery query)
        {
            var context = new TableStorageQueryContext
            {
                ContinuationInfo = new TableStorageContinuationInfo
                {
                    Query = query,
                    PageSize = query.TakeCount ?? DEFAULT_PAGE_SIZE,
                    HasMoreResult = true
                },
            };
            return await ExecuteQueryAsync(context).ConfigureAwait(false);
        }

        private async Task<StorageQueryResult<T>> ExecuteQueryAsync(TableStorageQueryContext queryContext)
        {
            var table = await TableAsync().ConfigureAwait(false);

            var result = await table.ExecuteQuerySegmentedAsync(queryContext.ContinuationInfo.Query, queryContext.ContinuationInfo.ContinuationToken).ConfigureAwait(false);
            queryContext.ContinuationInfo.ContinuationToken = result.ContinuationToken;
            queryContext.ContinuationInfo.HasMoreResult = result.ContinuationToken != null;
            var records = result.Results.Select(d => BSonConvert.DeserializeObject<T>(d.Properties["Content"].BinaryValue));

            return new StorageQueryResult<T>
            {
                QueryContext = BSonConvert.SerializeToBase64String(queryContext),
                Records = records,
                HasMoreResult = queryContext.HasMoreResult
            };
        }

        public Dictionary<string, IEnumerable<string>> GetPartitionsValues(T entity)
        {
            return EntityDefinition.Partitions().Where(p => !string.IsNullOrWhiteSpace(p.Name)).ToDictionary(k => k.Name, v => v.Expressions.Select(e => TableStorageQueryBuilder.NormalizeStringValue(e.Evaluate(entity) ?? "NULL")));
        }
    }
}