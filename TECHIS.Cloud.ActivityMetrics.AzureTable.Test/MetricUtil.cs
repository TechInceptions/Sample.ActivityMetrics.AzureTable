using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable
{
    public static class MetricUtil
    {
        #region Constants 
        public const string NAME_COUNTVALUE = "Count";
        public const decimal PARTITION_PAGESIZE = 50;
        public const string NUM_PADDING_999MILLION_MAX = "000000000"; //D19
        public const string NUM_PADDING_9999_MAX = "0000";
        
        public const string NAME_SUBJECTID_FIELD = nameof(ActivityEntityBase.SubjectId);
        public const string NAME_PARTITIONKEY = nameof(TableEntity.PartitionKey);
        #endregion


        public static List<string> GetPartitionKeys(int minSubjectId, int maxSubjectId, string subjectTypeId)
        {
            int min = minSubjectId, max = maxSubjectId;
            if (minSubjectId > maxSubjectId)
            {
                max = minSubjectId;
                min = maxSubjectId;
            }

            List<string> partitionKeys = new List<string>();

            //test upper/lower ceilings
            var upper = GetPartitionNumber(max);
            var lower = GetPartitionNumber(min);

            if (upper == lower)
            {
                //all in one partition
                partitionKeys.Add(DerivePartitionKey(upper, subjectTypeId));
            }
            else
            {
                //multi partitions
                for (decimal i = lower; i < upper; i++)
                {
                    partitionKeys.Add(DerivePartitionKey(i, subjectTypeId));
                }
            }

            return partitionKeys;
        }

        public static string DerivePartitionKey(int subjectId, string subjectTypeId)
        {
            return DerivePartitionKey(GetPartitionNumber(subjectId), subjectTypeId);
        }
        public static string DerivePartitionKey(decimal partitionNumber, string subjectTypeId)
        {
            return $"{subjectTypeId}:{partitionNumber.ToString(NUM_PADDING_999MILLION_MAX)}";
        }
        public static string DeriveRowKey(int subjectId, string subjectTypeId)
        {
            return $"{subjectTypeId}:{subjectId.ToString(NUM_PADDING_999MILLION_MAX)}";
        }
        public static decimal GetPartitionNumber(int subjectId)
        {
            return Math.Ceiling((subjectId / PARTITION_PAGESIZE));
        }
    }
}
