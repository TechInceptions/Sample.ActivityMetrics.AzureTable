using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable
{
    public class ActivityEntityBase: TableEntity
    {
        public int SubjectId { get; set; }
        public string  SubjectTypeId { get; set; }
        public string ValueType { get; set; }
    }
}
