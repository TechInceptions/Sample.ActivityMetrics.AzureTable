using System;
using System.Collections.Generic;
using Microsoft.Azure.CosmosDB.Table;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TECHIS.Cloud.ActivityMetrics.AzureTable.Test
{
    [TestClass]
    public class ActivityTrackerTests
    {
        string[] _PartitionKeys = { "A", "B", "C", "D" };
        string[] _RowKeys = { "01", "02", "03", "04" };
        private const string _JournalTableName = "TestJournal";
        IActivityTracker<DynamicTableEntity> _Journal = (new ActivityTrackerFactory<DynamicTableEntity>()).Create(_JournalTableName, (string)null);

        [TestMethod]
        public void Insert()
        {

            var journal = (new ActivityTrackerFactory<DynamicTableEntity>()).Create(_JournalTableName, (string)null);
            InsertByPartitions(_PartitionKeys, _RowKeys, _Journal);
        }
        [TestMethod]
        public void Delete()
        {

            var journal = (new ActivityTrackerFactory<DynamicTableEntity>()).Create(_JournalTableName, (string)null);
            DeleteByPartitions(_PartitionKeys, _RowKeys, journal);
        }
        [TestMethod]
        public async System.Threading.Tasks.Task InsertAsync()
        {

            var journal = (new ActivityTrackerFactory<DynamicTableEntity>()).Create(_JournalTableName, (string)null);
            await InsertByPartitionsAsync(_PartitionKeys, _RowKeys, _Journal);
        }
        [TestMethod]
        public async System.Threading.Tasks.Task DeleteAsync()
        {

            var journal = (new ActivityTrackerFactory<DynamicTableEntity>()).Create(_JournalTableName, (string)null);
            await DeleteByPartitionsAsync(_PartitionKeys, _RowKeys, journal);
        }

        [TestMethod]
        public void DeleteGlobal()
        {
            InsertByPartitions(_PartitionKeys, _RowKeys, _Journal);
            DeleteByPartitions(_PartitionKeys, _RowKeys, _Journal);
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
                    journal.Delete(new Identifier( entry, partition));
                }
            }
        }
        private static async System.Threading.Tasks.Task InsertByPartitionsAsync(string[] partitions, string[] entries, IActivityTracker<DynamicTableEntity> journal)
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
        private static async System.Threading.Tasks.Task DeleteByPartitionsAsync(string[] partitions, string[] entries, IActivityTracker<DynamicTableEntity> journal)
        {
            foreach (var partition in partitions)
            {
                foreach (var entry in entries)
                {
                    await journal.DeleteAsync(new Identifier( entry, partition));
                }
            }
        }
    }
}
