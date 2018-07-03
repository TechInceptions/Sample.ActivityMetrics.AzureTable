using Microsoft.Azure.CosmosDB.Table;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable.Test
{
    [TestClass]
    public class CountGeneratorUsage
    {
        [TestInitialize]
        public void Init()
        {
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.DefaultConnectionLimit = int.MaxValue;
        }

        private const string _JournalTableName = "ReactionJournal";
        private const string _SummaryTableName = "ReactionSummary";

        private const string _ValueName = "Count";

        private int[] _SubjectIds = { 101, 102, 103, 104, 106, 107, 108, 109, 110, 151, 152, 153, 154, 156, 157, 158, 159, 160, 1, 2, 3, 4, 6, 7, 8, 9, 10, 51, 52, 53, 54, 56, 57, 58, 59, 60 };

        private string[] _SubjectTypeIds = { "1", "2", "3", "4", "5" };

        private string[] _Actors = { "john", "manbij", "ike", "ari", "kina", "mary" };
        private string[] _ValueTypes = { "like", "smile", "dislike", "love", "hate", "upset" };
        private MetricsGeneratorFactory<long, EntityProperty, Configuration> _MetricGeneratorFactory = new MetricsGeneratorFactory<long, EntityProperty, Configuration>();

        private string CalculatorName = KnownCalculatorNames.Count;

        [TestMethod]
        public async Task UpdateTestAsync()
        {
            await PopulateJournalAsync(true);

            var newActivityFilters = await GetActivityFiltersValues();

            var sum = _MetricGeneratorFactory.Create((new CalculatorFactory<long>()).Create(CalculatorName,TableNames.Reactions, null, null), TableNames.Reactions, TableNames.Metrics, (string)null);

            List<Task<MetricGenerationResult>> sumTasks = new List<Task<MetricGenerationResult>>();
            foreach (var activityFilter in newActivityFilters)
            {

                sumTasks.Add(sum.GenerateAsync(new Identifier(rowKey: $"{activityFilter.SubjectId}:{activityFilter.SubjectTypeId}:{activityFilter.ValueType}",
                                                                partitionKey: MetricUtil.DerivePartitionKey(activityFilter.SubjectId, activityFilter.SubjectTypeId)),
                                                                MetricUtil.NAME_COUNTVALUE,
                                                                new QueryFilter(nameof(ActivityEntityBase.ValueType), activityFilter.ValueType),
                                                                new QueryFilter(nameof(ActivityEntityBase.SubjectId), activityFilter.SubjectId),
                                                                new QueryFilter(nameof(ActivityEntityBase.SubjectTypeId), activityFilter.SubjectTypeId)));
            }

            var results = await Task.WhenAll(sumTasks);
            

            Assert.IsTrue(results.All(p => p.ValuePersisted));
        }

        private IMetricsQueryable<TEntity> GetQuery<TEntity>() where TEntity : ITableEntity, new()
        {
            return new MetricQueryableFactory<TEntity>().Create((string)null);
        }
        private async Task<List<ActivityEntityBase>> GetActivityFiltersValues()
        {
            //Check history table timestamp of last processed
            var queryResult = await (GetQuery<MetricHistoryEntry>()).GetAsync(TableNames.CountHistory, 1);
            string queryPartitionKey;
            if (queryResult.Results.Count < 1)
            {
                queryPartitionKey = ActivityUtil.GetPartitionKey(DateTime.UtcNow.Subtract(new TimeSpan(30 * 3, 0, 0, 0)).Ticks);
            }
            else
            {
                queryPartitionKey = queryResult.Results[0].TargetPartitionKey;
            }

            QueryResult<ActivityEntityBase> newActivities = await (GetQuery<ActivityEntityBase>())
                                        .GetAsync(TableNames.Reactions, null,
                                        new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter(nameof(TableEntity.PartitionKey), queryPartitionKey), QueryComparisons.LessThan) });

            //Write the max timestamp for this session back to the history table
            var minPartitionKey = newActivities.Results.Min(p => p.PartitionKey);

            if (!string.IsNullOrEmpty(minPartitionKey))
            {
                await ((new ActivityTrackerFactory<MetricHistoryEntry>()).Create(TableNames.CountHistory,(string)null)).InsertAsync(new MetricHistoryEntry(minPartitionKey, newActivities.Results.Count));
            }

            return GetDistinct(newActivities.Results);
        }
        private List<ActivityEntityBase> GetDistinct(List<ActivityEntityBase> newActivities)
        {
            List<string> Index = new List<string>();

            for (int i = newActivities.Count - 1; i >= 0; i--)
            {
                var activity = newActivities[i];
                string key = $"{activity.SubjectId}-{activity.SubjectTypeId}-{activity.ValueType}";
                if (Index.Contains(key))
                {
                    newActivities.RemoveAt(i);
                }
                else
                {
                    Index.Add(key);
                }
            }

            return newActivities;
        }



        private string DeriveSummaryPartition(string subjectId, string subjectTypeId, decimal partitionPageSize = 50)
        {
            string partition = "1";
            if (int.TryParse(subjectId, out int result))
            {
                partition = $"{Math.Ceiling((result / partitionPageSize))}-{subjectTypeId}";
            }

            return partition;
        }


        private async Task<bool[]> PopulateJournalAsync(bool randomExecute = false, int randomChanceCount = 3, int randomPoolSize = 10)
        {
            var journal = new Activity();

            var numberOfCycles = 1;
            List<Task<bool>> tasks = new List<Task<bool>>();

            int bet = 0;
            if (randomExecute)
            {
                bet = (new Random()).Next(randomPoolSize);
            }
            for (int i = 0; i < numberOfCycles; i++)
            {
                foreach (var typeId in _SubjectTypeIds)
                {
                    foreach (var subjectId in _SubjectIds)
                    {
                        if ((!randomExecute) || RandomAllow(bet, randomChanceCount, randomPoolSize))
                        {
                            var partitionKey = $"{subjectId}-{typeId}";
                            tasks.Add(journal.AddAsync(subjectId, typeId, RandomSelect(new string[] { "1", "2", "3" }), RandomSelect(_ValueTypes), RandomSelect(_Actors)));
                        }
                    }
                }
            }

            return await Task.WhenAll(tasks);
        }

        private TResult RandomSelect<TResult>(IList<TResult> set)
        {
            return set[(new Random()).Next(set.Count)];
        }


        private bool RandomAllow(int chances = 3, int poolSize = 10)
        {
            Random rnd = new Random();

            int bet = rnd.Next(poolSize);

            List<int> numberPool = new List<int>(chances);
            for (int i = 0; i < chances; i++)
            {
                numberPool.Add(rnd.Next(poolSize));
            }

            if (numberPool.Contains(bet))
            {
                return true;
            }

            return false;
        }
        private bool RandomAllow(int bet, int chances = 3, int poolSize = 10)
        {
            Random rnd = new Random();


            List<int> numberPool = new List<int>(chances);
            for (int i = 0; i < chances; i++)
            {
                numberPool.Add(rnd.Next(poolSize));
            }

            if (numberPool.Contains(bet))
            {
                return true;
            }

            return false;
        }
    }
}
