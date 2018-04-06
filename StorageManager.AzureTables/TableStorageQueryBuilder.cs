using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.WindowsAzure.Storage.Table;
using StorageManager.Enums;
using StorageManager.Exceptions;
using StorageManager.Query;
using StorageManager.Storage;

namespace StorageManager.AzureTables
{
    public class TableStorageQueryBuilder
    {
        public const string MAIN_PARTITION_NAME = "MAIN_PARTITION";


        public const string PARTITION_FIELD_SEPARATOR = "__";
        public const string PARTITION_NAME_SEPARATOR = "::";
        const int DEFAULT_PAGE_SIZE = 1000;

        private readonly StorageEntityDefinition _entityDefinition;

        public static string NormalizeStringValue(object value)
        {
            if (value == null)
                return "NULL";

            if (value.GetType().IsEnum)
                return GetTableKeyNormalizedValue(value.ToString());

            if (value is IEnumerable && !(value is string))
            {
                List<string> strValues = new List<string>();
                var values = value as IEnumerable;
                foreach (var v in values)
                    strValues.Add(NormalizeStringValue(v));
                strValues.Sort();
                return GetTableKeyNormalizedValue(String.Join(";", strValues));
            }

            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.SByte:
                case TypeCode.Byte:
                    return $"{value:000}";
                case TypeCode.Int16:
                case TypeCode.UInt16:
                    return $"{value:00000}";
                case TypeCode.Int32:
                case TypeCode.UInt32:
                    return $"{value:0000000000}";
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return $"{value:00000000000000000000}";
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
                    return $"{value:00000000000000000000.0000000000}";
                case TypeCode.DateTime:
                    return ((DateTime)value).ToString("yyyyMMddHHmmssfff");
                default:
                    return value.ToString();
            }
        }

        public static string GetTableKeyNormalizedValue(string valueKey)
        {
            return Regex.Replace(valueKey, @"[\\/#?\t\n\r]", "|");
        }

        public static string GetPartitionKeyValue(string partitionName, IEnumerable<string> values)
        {
            string normalizeValueData = GetTableKeyNormalizedValue(String.Join(PARTITION_FIELD_SEPARATOR, values));

            if (normalizeValueData.Length > 128)
            {
                var partitionNameLength =
                    String.IsNullOrWhiteSpace(partitionName)
                        ? 0
                        : partitionName.Length + MAIN_PARTITION_NAME.Length;
                normalizeValueData = normalizeValueData.Substring(0, 128 - partitionNameLength);
            }
            return normalizeValueData;
        }

        public TableStorageQueryBuilder(string partitionName, StorageEntityDefinition entityDefinition)
        {
            PartitionValues = new List<StorageQueryConstantMember>();
            PartitionName = partitionName;
            Filters = new List<StorageComparisonExpression>();
            _entityDefinition = entityDefinition;
        }

        public List<StorageQueryConstantMember> PartitionValues { get; }

        public string PartitionName { get; }

        public List<StorageComparisonExpression> Filters { get; set; }

        public TableQuery BuildQuery(int? take)
        {
            string queryResult = null;
            string partitionQuery = null;

            if (PartitionValues.Any())
            {
                var keyValues = PartitionValues.Select(val => NormalizeStringValue(val?.Value) ?? "NULL");
                string normalizeValueData = GetPartitionKeyValue(PartitionName, keyValues);

                if (String.IsNullOrWhiteSpace(PartitionName))
                {
                    var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey",
                        QueryComparisons.Equal, MAIN_PARTITION_NAME);
                    var rowFilterLow = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, normalizeValueData);
                    var rowFilterHigh = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, normalizeValueData + "z");
                    partitionQuery = TableQuery.CombineFilters(partitionFilter, TableOperators.And,
                        TableQuery.CombineFilters(rowFilterLow, TableOperators.And, rowFilterHigh));
                }
                else
                {
                    var partitionValue = String.Join(PARTITION_NAME_SEPARATOR, PartitionName, normalizeValueData);
                    var partitionFilterLow = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, partitionValue);
                    var partitionFilterHigh = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, partitionValue + "z");
                    partitionQuery = TableQuery.CombineFilters(partitionFilterLow, TableOperators.And, partitionFilterHigh);
                }
            }


            var filterQuerys = BuildFilerQuery();
            if (String.IsNullOrWhiteSpace(filterQuerys))
            {
                queryResult = partitionQuery;
            }
            else
            {
                if (String.IsNullOrWhiteSpace(partitionQuery))
                    queryResult = filterQuerys;
                else
                    queryResult = TableQuery.CombineFilters(partitionQuery, TableOperators.And, filterQuerys);
            }


            return new TableQuery
            {
                FilterString = queryResult,
                TakeCount = take ?? DEFAULT_PAGE_SIZE
            };
        }

        private string BuildFilerQuery()
        {
            string queryFilters = null;
            foreach (var filter in Filters)
            {
                var fieldName = _entityDefinition.Filters().FirstOrDefault(f => f.Accesor.MemberPath == filter.EntityMember.MemberPath)?.Name;
                if (String.IsNullOrWhiteSpace(fieldName))
                    throw new StorageArgumentOutOfRangeException(nameof(filter), $"Not found filter with signature {filter.EntityMember.MemberPath}");

                string queryFilter = null;

                var toCompare = filter.ConstantMember.Value;
                switch (Type.GetTypeCode(toCompare.GetType()))
                {
                    case TypeCode.Boolean:
                        queryFilter = TableQuery.GenerateFilterConditionForBool(fieldName, GetOperator(filter.Operator), (bool) toCompare);
                        break;
                    case TypeCode.SByte:
                    case TypeCode.Byte:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                        queryFilter = TableQuery.GenerateFilterConditionForInt(fieldName, GetOperator(filter.Operator), (int)toCompare);
                        break;
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                        queryFilter = TableQuery.GenerateFilterConditionForLong(fieldName, GetOperator(filter.Operator), (long) toCompare);
                        break;
                    case TypeCode.Single:
                    case TypeCode.Double:
                    case TypeCode.Decimal:
                        queryFilter = TableQuery.GenerateFilterConditionForDouble(fieldName, GetOperator(filter.Operator), (double)toCompare);
                        break;
                    case TypeCode.DateTime:
                        queryFilter = TableQuery.GenerateFilterConditionForDate(fieldName, GetOperator(filter.Operator), (DateTime)toCompare);
                        break;
                    case TypeCode.Char:
                    case TypeCode.String:
                        queryFilter = TableQuery.GenerateFilterCondition(fieldName, GetOperator(filter.Operator), toCompare.ToString());
                        break;
                    default:
                        throw new StorageArgumentOutOfRangeException(nameof(filter), $"Can not extract value for type '{toCompare.GetType().FullName}'");
                }

                if (String.IsNullOrWhiteSpace(queryFilters))
                    queryFilters = queryFilter;
                else
                    queryFilters = TableQuery.CombineFilters(queryFilters, TableOperators.And, queryFilter);
            }
            return queryFilters;
        }

        private string GetOperator(QueryComparisonOperator filterOperator)
        {
            switch (filterOperator)
            {
                case QueryComparisonOperator.Equal:
                    return QueryComparisons.Equal;
                case QueryComparisonOperator.NotEqual:
                    return QueryComparisons.Equal;
                case QueryComparisonOperator.GreaterThan:
                    return QueryComparisons.Equal;
                case QueryComparisonOperator.GreaterThanOrEqual:
                    return QueryComparisons.Equal;
                case QueryComparisonOperator.LessThan:
                    return QueryComparisons.Equal;
                case QueryComparisonOperator.LessThanOrEqual:
                    return QueryComparisons.Equal;
                default:
                    throw new StorageArgumentOutOfRangeException(nameof(filterOperator), $"Operator {filterOperator} not supported yet");
            }
        }
    }
}