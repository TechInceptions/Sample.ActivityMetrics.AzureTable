using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TECHIS.Cloud.ActivityMetrics;
using TECHIS.Cloud.ActivityMetrics.AzureTable;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable
{
    public class Activity
    {
        private IActivityTracker<ActivityEntity> _ActivityTracker;

        public Activity() {
            _ActivityTracker = GetActivityTracker();
        }

        public async Task<bool> AddAsync(int subjectId, string subjectTypeId, string value, string valueType, string actor) {

            var entity = GetActivityEntity(subjectId, subjectTypeId, value, valueType, actor);
            
            return await _ActivityTracker.InsertAsync(entity);
        }

        //public async Task<List<Dictionary<string,string>>> QueryAsync(int subjectId, string subjectTypeId, string value, string valueType, string actor, int count    )
        //{
        //    IMetricsQueryable<DynamicTableEntity> query = new Query<DynamicTableEntity>((string)null);
        //    var result = await query.GetAsync(TableNames.Reactions, count, GetActivityValues(subjectId, subjectTypeId, value, valueType, actor).ToArray());

        //    return result.Convert();
        //}

        #region Private Methods 
        
        private ActivityEntity GetActivityEntity(int subjectId, string subjectTypeId, string value, string valueType, string actor)
        {
            return new ActivityEntity { Actor = actor, PartitionKey = ActivityUtil.GetPartitionKey(), RowKey = ActivityUtil.GetRowKey(subjectId, subjectTypeId), SubjectId = subjectId, SubjectTypeId = subjectTypeId, Value = value, ValueType = valueType };
        }

        //private List<QueryFilter> GetActivityValues(int subjectId, string subjectTypeId, string value, string valueType, string actor)
        //{
        //    var properties = new List<QueryFilter>(5);

        //    if (ActivityEntityBase.IsValidSubjectId((subjectId)))
        //        properties.Add( new QueryFilter( nameof(subjectId), subjectId));

        //    if (!string.IsNullOrEmpty(subjectTypeId))
        //        properties.Add(new QueryFilter(nameof(subjectTypeId), subjectTypeId));

        //    if (!string.IsNullOrEmpty(value))
        //        properties.Add(new QueryFilter(nameof(value), value));

        //    if (!string.IsNullOrEmpty(valueType))
        //        properties.Add(new QueryFilter(nameof(valueType), valueType));

        //    if (!string.IsNullOrEmpty(actor))
        //        properties.Add(new QueryFilter(nameof(actor), actor));

        //    return properties;
        //}


        private IActivityTracker<ActivityEntity> GetActivityTracker()
        {
            return (new ActivityTrackerFactory<ActivityEntity>()).Create(TableNames.Reactions, (string)null);
            //return new ActivityTracker<ActivityEntity>(TableNames.Reactions);
        }
        #endregion
    }
}
