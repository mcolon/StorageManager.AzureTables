using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.WindowsAzure.Storage.Table;
using StorageManager.Enums;
using StorageManager.Exceptions;
using StorageManager.Query;
using StorageManager.Storage;

namespace StorageManager.AzureTables
{
    internal class TableStorageQueryTranslator<T> : StorageQueryTranslator
    {
        protected readonly StorageEntityDefinition EntityDefinition;

        public TableStorageQueryTranslator(StorageEntityDefinition<T> entityDefinition)
        {
            EntityDefinition = entityDefinition;
        }

        public new TableQuery Translate(Expression expression)
        {
            var toNormalize = base.Translate(expression);
            if (toNormalize == null)
            {
                return new TableQuery
                {
                    FilterString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TableStorageQueryBuilder.MAIN_PARTITION_NAME),
                    TakeCount = Take
                };
            }

            var normalizedQuery = Normalize(toNormalize);

            return SplitQueryExpression(normalizedQuery);
        }

        public TableQuery QueryGetById(string id)
        {
            return GetQueryById(id);
        }

        public static TableQuery GetQueryById(string id)
        {
            var query = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TableStorageQueryBuilder.MAIN_PARTITION_NAME),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, id));

            return new TableQuery
            {
                FilterString = query,
                SelectColumns = new[] { "Content" },
                TakeCount = 1
            };
        }


        private TableQuery SplitQueryExpression(StorageQueryOperand query)
        {
            string result = null;
            List<List<StorageComparisonExpression>> conditionGroups = ExtractConditionGroups(query);
            var contitions = conditionGroups.Select(BuildQueryExpression);

            foreach (var qry in contitions)
            {
                if (result == null)
                    result = qry.FilterString;
                else
                    result = TableQuery.CombineFilters(result, TableOperators.Or, qry.FilterString);
            }

            return new TableQuery
            {
                FilterString = result,
                SelectColumns = new[] {"Content"},
                TakeCount = Take
            };
        }

        private TableQuery BuildQueryExpression(List<StorageComparisonExpression> grp)
        {
            var partitionAccessData = MatchPartitionAndFilters(grp);
            return partitionAccessData.BuildQuery(Take);
        }

        private TableStorageQueryBuilder MatchPartitionAndFilters(List<StorageComparisonExpression> grp)
        {
            var partitions = EntityDefinition.Partitions();
            var equalConditions = grp.Where(c => c.Operator == QueryComparisonOperator.Equal);


            TableStorageQueryBuilder candidatePartition = null;
            foreach (var partition in partitions)
            {
                TableStorageQueryBuilder current = new TableStorageQueryBuilder(partition.Name, EntityDefinition);
                List<string> selectedFields = new List<string>();
                foreach (var fld in partition.Expressions)
                {
                    var qryField = equalConditions.FirstOrDefault(f => f.EntityMember.MemberPath == fld.MemberPath);
                    if (qryField != null)
                    {
                        current.PartitionValues.Add(qryField.ConstantMember);
                        selectedFields.Add(qryField.EntityMember.MemberPath);
                    }
                    else
                        break;
                }
                current.Filters.AddRange(equalConditions.Where(c => selectedFields.All(s => s != c.EntityMember.MemberPath)));

                if (candidatePartition == null || candidatePartition.PartitionValues.Count < current.PartitionValues.Count)
                    candidatePartition = current;
            }

            candidatePartition.Filters.AddRange(grp.Where(c => c.Operator != QueryComparisonOperator.Equal));

            if (candidatePartition == null && !EntityDefinition.AllowTableScan)
                throw new StorageInvalidOperationException($"Not found partitions for expression {String.Join(" And ", grp)}");

            return candidatePartition;
        }

        private List<List<StorageComparisonExpression>> ExtractConditionGroups(StorageQueryOperand normalizedQuery)
        {
            var result = new List<List<StorageComparisonExpression>>();
            if (normalizedQuery is StorageLogicalExpression logical)
            {
                if (logical.Operator == QueryLogicalOperator.And)
                {
                    result.Add(ProcessLogicalAndOperand(logical));
                }
                else
                {
                    ProcessLogicalOrOperand(logical, result);
                }
            }
            else
            {
                result.Add(new List<StorageComparisonExpression> {(StorageComparisonExpression) normalizedQuery});
            }
            return result;
        }

        private void ProcessLogicalOrOperand(StorageLogicalExpression operand, List<List<StorageComparisonExpression>> conditionList)
        {
            ProcessOrOperandTree(operand.LeftOperand, conditionList);
            ProcessOrOperandTree(operand.RightOperand, conditionList);
        }

        private List<StorageComparisonExpression> ProcessLogicalAndOperand(StorageLogicalExpression logical)
        {
            var result = ProcessAndOperandTree(logical.LeftOperand);
            result.AddRange(ProcessAndOperandTree(logical.RightOperand));
            return result;
        }

        private List<StorageComparisonExpression> ProcessAndOperandTree(StorageQueryOperand logical)
        {
            List<StorageComparisonExpression> result = new List<StorageComparisonExpression>();
            if (logical is StorageComparisonExpression condition)
                result.Add(condition);
            else
            {
                if (((StorageLogicalExpression) logical).Operator == QueryLogicalOperator.Or)
                    throw new StorageArgumentOutOfRangeException(nameof(logical), "Expression not normalized");
                result.AddRange(ProcessLogicalAndOperand((StorageLogicalExpression)logical));
            }
            return result;
        }

        private void ProcessOrOperandTree(StorageQueryOperand logical, List<List<StorageComparisonExpression>> conditionList)
        {
            List<StorageComparisonExpression> result = new List<StorageComparisonExpression>();

            if (logical is StorageComparisonExpression condition)
                result.Add(condition);
            else
            {
                if (((StorageLogicalExpression) logical).Operator == QueryLogicalOperator.And)
                    conditionList.Add(ProcessLogicalAndOperand((StorageLogicalExpression) logical));
                else
                    ProcessLogicalOrOperand((StorageLogicalExpression) logical, conditionList);
            }
        }

        private StorageQueryOperand Normalize(StorageQueryOperand toNormalize)
        {
            if (toNormalize is StorageLogicalExpression logical)
            {
                var left = Normalize(logical.LeftOperand);
                var right = Normalize(logical.RightOperand);

                if (logical.Operator == QueryLogicalOperator.Or)
                    return new StorageLogicalExpression(left, right, QueryLogicalOperator.Or);

                if (logical.Operator == QueryLogicalOperator.And)
                {
                    if (left is StorageLogicalExpression leftResult && leftResult.Operator == QueryLogicalOperator.Or)
                    {
                        var leftOperand = Normalize(new StorageLogicalExpression(right, leftResult.LeftOperand, QueryLogicalOperator.And));
                        var rightOperand = Normalize(new StorageLogicalExpression(right, leftResult.RightOperand, QueryLogicalOperator.And));
                        return new StorageLogicalExpression(leftOperand, rightOperand, QueryLogicalOperator.Or);
                    }

                    if (right is StorageLogicalExpression rightResult && rightResult.Operator == QueryLogicalOperator.Or)
                    {
                        var leftOperand = Normalize(new StorageLogicalExpression(left, rightResult.LeftOperand, QueryLogicalOperator.And));
                        var rightOperand = Normalize(new StorageLogicalExpression(left, rightResult.RightOperand, QueryLogicalOperator.And));
                        return new StorageLogicalExpression(leftOperand, rightOperand, QueryLogicalOperator.Or);
                    }
                    return new StorageLogicalExpression(left, right, QueryLogicalOperator.And);
                }
            }
            return toNormalize;
        }
    }
}