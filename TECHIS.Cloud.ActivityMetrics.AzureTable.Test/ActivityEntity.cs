using System;
using System.Collections.Generic;
using System.Text;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable
{
    public class ActivityEntity:ActivityEntityBase
    {
        public string Value { get; set; }
        public string Actor { get; set; }
    }
}
