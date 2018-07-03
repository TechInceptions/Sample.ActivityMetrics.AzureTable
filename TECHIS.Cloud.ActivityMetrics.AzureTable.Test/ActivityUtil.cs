using System;
using System.Collections.Generic;
using System.Text;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable
{
    public static class ActivityUtil
    {
        private static long _MaxDate = new DateTimeOffset(3018, 1, 1, 0, 0, 0, new TimeSpan(0)).DateTime.Ticks;
        public static string GetPartitionKey(long? currentTimeTicks = null, string stringFormat= "D19")
        {
            return GetDescendingOrderKey(currentTimeTicks).ToString(stringFormat);
        }
        public static string FormatPartitionKey(long currentTimeTicks, string stringFormat = "D19")
        {
            return currentTimeTicks.ToString(stringFormat);
        }
        public static long GetDescendingOrderKey(long? currentTimeTicks=null)
        {
            if (currentTimeTicks.HasValue)
            {
                return _MaxDate - currentTimeTicks.Value;
            }
            else
            {
                return (_MaxDate - DateTime.UtcNow.Ticks);
            }
        }
        public static string GetRowKey(string subjectId, string subjectTypeId)
        {
            return $"{subjectId}:{subjectTypeId}";
        }
        public static string GetRowKey(int subjectId, string subjectTypeId)
        {
            return $"{subjectId}:{subjectTypeId}";
        }
    }
}
