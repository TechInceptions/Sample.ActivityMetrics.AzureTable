using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB.Table;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable.Test
{
    [TestClass]
    public class QueryTests
    {
        string[] _PartitionKeys = { "A", "B", "C", "D" };
        string[] _RowKeys = { "01", "02", "03", "04" };
        private const string _JournalTableName = "TestJournal2";
        IActivityTracker<DynamicTableEntity> _Journal = new ActivityTrackerFactory<DynamicTableEntity>().Create(_JournalTableName,(string)null); //new ActivityTracker<DynamicTableEntity>(_JournalTableName);

        [TestMethod]
        public async Task CRUDGlobalAsync()
        {
            var query = new MetricQueryableFactory<DynamicTableEntity>().Create((string)null);
            var hasRows = (await query.GetAsync(_JournalTableName, 1)).Results.Count>0;

            if (hasRows)
            {
                var allRows = await query.GetAsync(_JournalTableName);

                foreach (var row in allRows.Results)
                {
                    await _Journal.DeleteAsync(new Identifier( row.RowKey, row.PartitionKey));
                }
            }

            //Create
            await InsertByPartitionsAsync(_PartitionKeys, _RowKeys, _Journal);

            //Read 1
            var rows = await query.GetAsync(_JournalTableName, null, new QueryFilter("PartitionKey", "A"), new QueryFilter("RowKey", "01"));
            Assert.IsTrue(rows.Results.Count == 1, "Wrong number of rows returned - 1");

            //Read 2
            var rows2 = await query.GetAsync(_JournalTableName, null, new QueryFilter("RowKey", "01"));
            Assert.IsTrue(rows2.Results.Count == _PartitionKeys.Length, "Wrong number of rows returned - 2");

            //Read 2.5
            var rows25 = await query.GetAsync(_JournalTableName, 1, new QueryFilter("RowKey", "01"));
            Assert.IsTrue(rows25.Results.Count == 1, "Wrong number of rows returned - 2.5");

            //Read 3
            var rows3 = await query.GetAsync(_JournalTableName, null, new QueryFilter("PartitionKey", "A"));
            Assert.IsTrue(rows3.Results.Count == _RowKeys.Length, "Wrong number of rows returned - 3");

            //Delete 
            await DeleteByPartitionsAsync(_PartitionKeys, _RowKeys, _Journal);

        }
        [TestMethod]
        public async Task TestQueryGlobalAsync()
        {
            var query = new MetricQueryableFactory<DynamicTableEntity>().Create((string)null);
            var hasRows = (await query.GetAsync(_JournalTableName, 1)).Results.Count > 0;

            if (hasRows)
            {
                var allRows = await query.GetAsync(_JournalTableName);

                foreach (var row in allRows.Results)
                {
                    await _Journal.DeleteAsync(new Identifier(row.RowKey, row.PartitionKey));
                }
            }

            //Create
            await InsertByPartitionsAsync(_PartitionKeys, _RowKeys, _Journal);

            //Read
            var rows = await query.GetAsync(_JournalTableName, 1, 
                new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.Equal),
                                        new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.Equal) }, TableOperators.And);
            Assert.IsTrue(rows.Results.Count == 1, "Wrong number of rows returned");

            //Read all but 1
            var rowsA = await query.GetAsync(_JournalTableName, null,
                    new QueryFilterCondition[] {    new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.NotEqual),
                                                    new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.NotEqual) }, TableOperators.And);
            int count = rowsA.Results.Count;
            Assert.IsTrue(rowsA.Results.Count == ((_PartitionKeys.Length-1) * (_RowKeys.Length-1)), "Wrong number of rows returned");

            //Read all but 1
            var rowsB = await query.GetAsync(_JournalTableName, null,
                new QueryFilterCondition[] {    new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.NotEqual),
                                                new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.NotEqual) }, TableOperators.Or);
            Assert.IsTrue(rowsB.Results.Count == ((_PartitionKeys.Length) * (_RowKeys.Length))-1, "Wrong number of rows returned");

            //Read all but 1
            var rowsC = await query.GetAsync(_JournalTableName, null,
                new QueryFilterCondition[] {    new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.NotEqual),
                                                new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.NotEqual),
                                                new QueryFilterCondition(new QueryFilter("RowKey", "10"), QueryComparisons.LessThan) }, TableOperators.Or, TableOperators.And);
            Assert.IsTrue(rowsC.Results.Count == ((_PartitionKeys.Length) * (_RowKeys.Length)) - 1, "Wrong number of rows returned");

            //Read
            var rows2 = await query.GetAsync(_JournalTableName, null, 
                new QueryFilterCondition[] {    new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.Equal) });
            Assert.IsTrue(rows2.Results.Count == _PartitionKeys.Length, "Wrong number of rows returned");


            //Read
            var rows2A = await query.GetAsync(_JournalTableName, null, "RowKey eq '01'");
            Assert.IsTrue(rows2A.Results.Count == _PartitionKeys.Length, "Wrong number of rows returned");

            //Read
            var rows3 = await query.GetAsync(_JournalTableName, null, new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.Equal) });
            Assert.IsTrue(rows3.Results.Count == _RowKeys.Length, "Wrong number of rows returned");

            //Update
            await InsertByPartitionsAsync(_PartitionKeys, _RowKeys, _Journal);

            //Delete
            await DeleteByPartitionsAsync(_PartitionKeys, _RowKeys, _Journal);

        }

        [TestMethod]
        public void TestQueryGlobal()
        {
            var query = new MetricQueryableFactory<DynamicTableEntity>().Create((string)null);
            var hasRows = ( query.Get(_JournalTableName, 1)).Results.Count > 0;

            if (hasRows)
            {
                var allRows =  query.Get(_JournalTableName);

                foreach (var row in allRows.Results)
                {
                     _Journal.Delete(new Identifier(row.RowKey, row.PartitionKey));
                }
            }

            //Create
             InsertByPartitions(_PartitionKeys, _RowKeys, _Journal);

            //Read
            var rows =  query.Get(_JournalTableName, 1,
                new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.Equal),
                                             new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.Equal) }, TableOperators.And);
            Assert.IsTrue(rows.Results.Count == 1, $"1. Wrong number of rows returned. Row count: {rows.Results.Count}");

            //Read all but 1
            var rowsA =  query.Get(_JournalTableName, null,
                    new QueryFilterCondition[] {    new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.NotEqual),
                                                    new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.NotEqual) }, TableOperators.And);
            Assert.IsTrue(rowsA.Results.Count == ((_PartitionKeys.Length - 1) * (_RowKeys.Length - 1)), $"2. Wrong number of rows returned. Row count: {rowsA.Results.Count}");

            //Read all but 1
            var rowsB =  query.Get(_JournalTableName, null,
                new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.NotEqual),
                                        new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.NotEqual) }, TableOperators.Or);
            Assert.IsTrue(rowsB.Results.Count == ((_PartitionKeys.Length) * (_RowKeys.Length)) - 1, $"3. Wrong number of rows returned. Row count: {rowsB.Results.Count}");

            //Read all but 1
            var rowsC =  query.Get(_JournalTableName, null,
                new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.NotEqual),
                                        new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.NotEqual),
                                        new QueryFilterCondition(new QueryFilter("RowKey", "10"), QueryComparisons.LessThan) }, TableOperators.Or, TableOperators.And);
            Assert.IsTrue(rowsC.Results.Count == ((_PartitionKeys.Length) * (_RowKeys.Length)) - 1, $"4. Wrong number of rows returned. Row count: {rowsC.Results.Count}");

            //Read
            var rows2 =  query.Get(_JournalTableName, null, new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.Equal) });
            Assert.IsTrue(rows2.Results.Count == _PartitionKeys.Length, $"5. Wrong number of rows returned. Row count: {rows2.Results.Count}");


            //Read
            var rows2A =  query.Get(_JournalTableName, null, "RowKey eq '01'");
            Assert.IsTrue(rows2A.Results.Count == _PartitionKeys.Length, $"6. Wrong number of rows returned. Row count: {rows2A.Results.Count}");

            //Read
            var rows3 =  query.Get(_JournalTableName, null, new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "A"), QueryComparisons.Equal) });
            Assert.IsTrue(rows3.Results.Count == _RowKeys.Length, $"7. Wrong number of rows returned. Row count: {rows3.Results.Count}");

            //Delete
             DeleteByPartitions(_PartitionKeys, _RowKeys, _Journal);

        }

        [TestMethod]
        public void TestQueryUtilCombiner()
        {
            var query = new MetricQueryableFactory<DynamicTableEntity>().Create((string)null);
            var hasRows = (query.Get(_JournalTableName, 1)).Results.Count > 0;

            if (hasRows)
            {
                var allRows = query.Get(_JournalTableName);

                foreach (var row in allRows.Results)
                {
                    _Journal.Delete(new Identifier(row.RowKey, row.PartitionKey));
                }
            }

            //Create
            InsertByPartitions(_PartitionKeys, _RowKeys, _Journal);
            
            //Read all but 1

            var table = QueryUtil<DynamicTableEntity>.Query(null, new string[] { TableOperators.Or, TableOperators.Or },
            (   new QueryFilterCondition[] {
                new QueryFilterCondition(new QueryFilter("PartitionKey","A"),QueryComparisons.Equal ),
                new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.GreaterThan),
                new QueryFilterCondition(new QueryFilter("RowKey", "10"), QueryComparisons.LessThan) }, 
                new string[] { TableOperators.And, TableOperators.And } ),
            (   new QueryFilterCondition[] { new QueryFilterCondition(new QueryFilter("PartitionKey", "B"), QueryComparisons.Equal),
                new QueryFilterCondition(new QueryFilter("RowKey", "01"), QueryComparisons.GreaterThan),
                new QueryFilterCondition( new QueryFilter("RowKey", "10"), QueryComparisons.LessThan) },
                new string[] { TableOperators.And, TableOperators.And })
            );


            //Delete
            DeleteByPartitions(_PartitionKeys, _RowKeys, _Journal);

            Assert.IsNotNull(table);


        }
        private static void InsertByPartitions(string[] partitions, string[] entries, IActivityTracker<DynamicTableEntity> journal)
        {
            foreach (var partition in partitions)
            {
                foreach (var entry in entries)
                {
                    DynamicTableEntity dt = new DynamicTableEntity(partition, entry);

                    const int itemCount = 6;
                    Dictionary<string, EntityProperty> valuePairs = new Dictionary<string, EntityProperty>(itemCount);
                    for (int i = 0; i < itemCount; i++)
                    {
                        valuePairs["p" + i.ToString()] = new EntityProperty( $"{i} - {DateTime.Now.ToLongTimeString()}" );
                    }
                    dt.Properties = valuePairs;
                    journal.Insert(dt);
                }
            }
        }
        private static void DeleteByPartitions(string[] partitions, string[] entries, IActivityTracker<DynamicTableEntity> journal)
        {
            foreach (var partition in partitions)
            {
                foreach (var entry in entries)
                {
                    journal.Delete(new Identifier(entry, partition));
                }
            }
        }
        private static async Task InsertByPartitionsAsync(string[] partitions, string[] entries, IActivityTracker<DynamicTableEntity> journal)
        {
            foreach (var partition in partitions)
            {
                foreach (var entry in entries)
                {
                    DynamicTableEntity dt = new DynamicTableEntity(partition, entry);

                    const int itemCount = 6;
                    Dictionary<string, EntityProperty> valuePairs = new Dictionary<string, EntityProperty>(itemCount);
                    for (int i = 0; i < itemCount; i++)
                    {
                        valuePairs["p" + i.ToString()] = new EntityProperty($"{i} - {DateTime.Now.ToLongTimeString()}");
                    }
                    dt.Properties = valuePairs;
                    await journal.InsertAsync(dt);
                }
            }
        }
        private static async Task DeleteByPartitionsAsync(string[] partitions, string[] entries, IActivityTracker<DynamicTableEntity> journal)
        {
            foreach (var partition in partitions)
            {
                foreach (var entry in entries)
                {
                    await journal.DeleteAsync(new Identifier(entry, partition));
                }
            }
        }
    }
}
