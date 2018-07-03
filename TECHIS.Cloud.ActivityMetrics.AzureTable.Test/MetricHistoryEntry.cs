using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable
{
    public class MetricHistoryEntry : TableEntity
    {
        public MetricHistoryEntry() { }
        public MetricHistoryEntry(string targetPartitionKey, int activityCount)
        {
            TargetPartitionKey = targetPartitionKey;
            ActivityCount = activityCount;

            PartitionKey = TargetPartitionKey;
            RowKey = string.Empty;
        }



        public string TargetPartitionKey { get; set; }
        public int ActivityCount { get; set; }
    }
}
