//using System;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using TECHIS.Cloud.Configuration;

//namespace TECHIS.Cloud.ActivityMetrics.AzureTable.Test
//{
//    [TestClass]
//    public class Connections
//    {
//        [TestMethod]
//        public async System.Threading.Tasks.Task TestConnectToConfigAsync()
//        {

//            JsonAppConfigProvider JsonAppConfig = new JsonAppConfigProvider("reactions.tables");
//            Configuration config = (await JsonAppConfig.GetAsync<Configuration>().ConfigureAwait(false)) ?? throw new InvalidOperationException("Failed to create config object");
//            Assert.IsNotNull(config);
//        }
//        [TestMethod]
//        public void TestConnect()
//        {
//            var storageAccount = CloudTableUtil.GetStorageAccountAsync().Result;

//            Assert.IsNotNull(storageAccount, "storageAccount not created");
//        }
//    }
//}
