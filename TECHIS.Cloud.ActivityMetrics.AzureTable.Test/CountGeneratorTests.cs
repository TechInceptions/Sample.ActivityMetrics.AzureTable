//using Microsoft.Azure.CosmosDB.Table;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Linq;
//using System.Net;
//using System.Text;
//using System.Threading.Tasks;

//namespace TECHIS.Cloud.ActivityMetrics.AzureTable.Test
//{
//    [TestClass]
//   public class CountGeneratorTests
//    {
//        [TestInitialize]
//        public void Init()
//        {
//            ServicePointManager.UseNagleAlgorithm = false;
//            ServicePointManager.Expect100Continue = false;
//            ServicePointManager.DefaultConnectionLimit = int.MaxValue;
//        }

//        private const string _JournalTableName = "ReactionJournal";
//        private const string _SummaryTableName = "ReactionSummary";

//        private const string _ValueName = "Count";

//        private string[] _SubjectIds =      { "101", "102", "103", "104", "106", "107", "108", "109", "110", "151", "152", "153", "154", "156", "157", "158", "159", "160", "1", "2", "3", "4", "6", "7","8","9","10", "51", "52", "53", "54", "56", "57", "58", "59", "60" };

//        private string[] _SubjectTypeIds =  { "1", "2", "3", "4", "5" };

//        private string[] _Actors = { "john", "manbij", "ike", "ari", "leena", "mary" };
//        private string[] _ValueTypes = { "like", "smile", "dislike", "love", "hate", "upset" };
//        [TestMethod]
//        public async Task PopulateAndUpdateAsync()
//        {
//            var sum = new CountGenerator(_JournalTableName, _SummaryTableName,(string)null);

//            PopulateJournal();
//            await ExecuteSummries(sum);
//        }
//        [TestMethod]
//        public async Task UpdateAsync()
//        {
//            var sum = new CountGenerator(_JournalTableName, _SummaryTableName, (string)null);

//            await ExecuteSummries(sum);
//        }
//        [TestMethod]
//        public async Task PopulateAndUpdateBatchAsync()
//        {
//            Stopwatch stopwatch = new Stopwatch();
//            stopwatch.Start();
//            PopulateJournal();
//            stopwatch.Stop();
//            var popTIme = stopwatch.Elapsed.TotalSeconds;
//            stopwatch.Reset();

//            var batch = CreateSummriesBatch();

//            stopwatch.Start();
//            var results = await new CountGenerator(_JournalTableName, _SummaryTableName, (string)null).GenerateAsync(batch);
//            stopwatch.Stop();
//            var batchTime = stopwatch.Elapsed.TotalSeconds;

//            Assert.IsTrue(results.All(r => r.ValuePersisted));
//        }
//        [TestMethod]
//        public async Task UpdateBatchAsync()
//        {
//            await new CountGenerator(_JournalTableName, _SummaryTableName, (string)null).GenerateAsync(CreateSummriesBatch()) ;
//        }

//        private async Task ExecuteSummries(CountGenerator sum)
//        {
//            foreach (var vType in _ValueTypes)
//            {
//                var valueType = new QueryFilter("valueType", vType);
//                foreach (var typeId in _SubjectTypeIds)
//                {
//                    var tId = new QueryFilter("typeId", typeId);
//                    foreach (var subjectId in _SubjectIds)
//                    {
//                        var sId = new QueryFilter("subjectId", subjectId);

//                        await sum.GenerateAsync( new Identifier($"{subjectId}-{typeId}-{vType}", DeriveSummaryPartition(subjectId, typeId)),  _ValueName, valueType, sId, tId);
//                    }
//                }
//            }
//        }

//        private GenerateMetricsInputs<long> CreateSummriesBatch()
//        {
//            GenerateMetricsInputs<long> list = new GenerateMetricsInputs<long>();
//            foreach (var vType in _ValueTypes)
//            {
//                var valueType = new KeyValuePair<string, string>("valueType", vType);
//                foreach (var typeId in _SubjectTypeIds)
//                {
//                    var tId = new KeyValuePair<string, string>("typeId", typeId);
//                    foreach (var subjectId in _SubjectIds)
//                    {
//                        var sId = new KeyValuePair<string, string>("subjectId", subjectId);
                        
//                        list.Add(DeriveSummaryPartition(subjectId, typeId), $"{subjectId}-{typeId}-{vType}", _ValueName, valueType, sId, tId);
//                    }
//                }
//            }

//            return list;
//        }
//        private string DeriveSummaryPartition(string subjectId, string subjectTypeId, decimal partitionPageSize = 50)
//        {
//            string partition = "1";
//            if (int.TryParse(subjectId, out int result))
//            {
//                partition = $"{Math.Ceiling((result / partitionPageSize))}-{subjectTypeId}";
//            }

//            return partition;
//        }
//        private void PopulateJournal()
//        {
//            var journal = new ActivityTracker<DynamicTableEntity>(_JournalTableName);

//            var numberOfCycles = 3;

//            for (int i = 0; i < numberOfCycles; i++)
//            {
//                foreach (var typeId in _SubjectTypeIds)
//                {
//                    var tId = new KeyValuePair<string, string>("typeId", typeId);
//                    foreach (var subjectId in _SubjectIds)
//                    {
//                        var sId = new KeyValuePair<string, string>("subjectId", subjectId);

//                        var partitionKey = $"{subjectId}-{typeId}";
//                        var rowKey = DateTime.UtcNow.Ticks.ToString();
//                        var value = new KeyValuePair<string, string>("value", RandomSelect(new string[] { "1", "2", "3" }));
//                        var actor = new KeyValuePair<string, string>("actor", RandomSelect(_Actors));
//                        var valueType = new KeyValuePair<string, string>("valueType", RandomSelect(_ValueTypes));

//                        DynamicTableEntity dynamicTableEntity = new DynamicTableEntity(partitionKey, rowKey);
//                        dynamicTableEntity.Properties = CloudTableUtil.Convert(new KeyValuePair<string, string>[] { value, actor, valueType, sId, tId });

//                        journal.Insert(dynamicTableEntity);
//                    }
//                }
//            }
//        }

//        private TResult RandomSelect<TResult>(IList<TResult> set)
//        {
//            return set[(new Random()).Next(set.Count)];
//        }
//    }
//}
