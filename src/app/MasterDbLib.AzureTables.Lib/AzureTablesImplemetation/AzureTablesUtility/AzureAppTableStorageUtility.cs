using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace MasterDbLib.AzureTables.Lib.DataBaseServices.AzureTablesImplemetation.AzureTablesUtility
{
    public class AzureAppTableStorageUtility
    {
        private readonly CloudStorageAccount _storageAccount;

        public string TestString = "";

        public AzureAppTableStorageUtility(string connectionString, string test = null)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));
            try
            {
                this.TestString = test ?? "";
                // Retrieve the storage account from the connection string.
                this._storageAccount = CloudStorageAccount.Parse(connectionString);
                // Create the table client.
                this.tableClient = this._storageAccount.CreateCloudTableClient();
            }
            catch (Exception e)
            {
                this.HandleException(e, $"Error initializing storage test {test}");
            }
        }

        private CloudTableClient tableClient { get; }

        public void HandleException(
            Exception e,
            string description = null,
            [CallerMemberName] string memberName = "",
            [CallerFilePath] string fileName = "",
            [CallerLineNumber] int lineNumber = 0)
        {
        }

        public async Task<bool> Execute<TCustomerEntity>(
            Action<TableBatchOperation, TCustomerEntity> operation,
            string tableName,
            List<TCustomerEntity> entities,
            int? tuneSpeed = null)
            where TCustomerEntity : AppTableEntity
        {
            var tasks = new List<Task<bool>>();
            IEnumerable<IGrouping<string, TCustomerEntity>> group = entities.GroupBy(x => x.PartitionKey);
            foreach (IGrouping<string, TCustomerEntity> t in group)
                tasks.Add(this.ExecuteInternal(operation, tableName, t.ToList(), tuneSpeed));

            await Task.WhenAll(tasks);

            return true;
        }

        private async Task<bool> ExecuteInternal<TCustomerEntity>(
            Action<TableBatchOperation, TCustomerEntity> operation,
            string tableName,
            List<TCustomerEntity> entities,
            int? tuneSpeed = null)
            where TCustomerEntity : AppTableEntity
        {
            CloudTable table = this.TableClientGetTableReference(tableName);
          await  table.CreateIfNotExistsAsync().ConfigureAwait(false);
            int taskCount = 0;
            int taskThreshold = tuneSpeed ?? 200; // Seems to be a good value to start with
            var batchTasks = new List<Task<IList<TableResult>>>();

            for (int i = 0; i < entities.Count; i += 99)
            {
                taskCount++;

                List<TCustomerEntity> batchItems = entities.Skip(i)
                    .Take(99)
                    .ToList();

                var batch = new TableBatchOperation();
                foreach (TCustomerEntity item in batchItems)
                    operation(batch, item);

                Task<IList<TableResult>> task = table.ExecuteBatchAsync(batch);

                batchTasks.Add(task);

                if (taskCount >= taskThreshold)
                {
                    await Task.WhenAll(batchTasks).ConfigureAwait(false);
                    taskCount = 0;
                }
            }

            await Task.WhenAll(batchTasks);
            return true;
        }

        public CloudTable TableClientGetTableReference(string tableName)
        {
            try
            {
                //https://martincarlsen.com/boost-your-azure-table-storage-performance/
                /*
                 SERVICE POINT MANAGER
Every operation performed against the Azure storage seems to imply one or more network hops.
Most of them are small HTTP requests to a single endpoint address. By default the .NET framework is very restrictive
with this kind of communication. By overriding the settings exposed by the ServicePointManager it is possible to make
.NET less restrictive and thus gaining improved performance. Note that those settings must be applied before the application makes any outbound connections.

There are three main settings. The first one is the DefaultConnectionLimit which has a default value of 2.
A higher value allows to do more requests in parallel. The optimal value for this depends on your application
and the surrounding environment and of course on the endpoint itself. There are some recommendations around which
could be a great starting point, but I think you´ll be better of at the end, trying different values to find the magic threshold for your environment.

The other setting to pay attention to is the Nagle´s algoritm. It is used to reduce network traffic and to increase
performance in TCP/IP based networks and by default this setting is on. But it has a negative effect on performance
when using Azure storage services. So try setting UseNagleAlgoritm property to false on the ServicePointManager.

The third setting is the 100-Continue behavior. By default this is on, which tells the client to wait for a HTTP
status 100 Continue from the server. Disabling this may result in better performance when using Azure storage services.

So to summarize those settings in code:

// This code applies the settings to the table storage endpoint only
var tableServicePoint = ServicePointManager.FindServicePoint(myCloudStorageAccount.TableEndpoint);
tableServicePoint.UseNagleAlgorithm = false;
tableServicePoint.Expect100Continue = false;
tableServicePoint.ConnectionLimit = 100;
With this settings the insert operation performance doubles: 24 items/second. But still not close to a great performance.
In the next code snippet we´ll take advantage of the increased connection limit, applied to the connection point, using Parallelism,
to make iterations over the dataset to run in parallel. The MaxDegreeOfParallelism property is set to 20, because higher values above
seems not to increase performance. This could of course be different in your environment.

Parallel.ForEach(entities, new ParallelOptions { MaxDegreeOfParallelism = 20 }, (entity) =>
{
    myCloudTable.Execute(TableOperation.Insert(entity));
});
Using the parallel approach the insert operation performance reached 240 items/second. So that increased performance up to ten times.
                 */
                //https://blogs.msmvps.com/nunogodinho/2013/11/20/windows-azure-storage-performance-best-practices/
                //https://stackoverflow.com/a/19449081/2124293
                //This is a notorious issue that has affected many developers. By default, the value
                //for the number of .NET HTTP connections is 2.
                //This implies that only 2 concurrent connections can be maintained. This manifests itself
                //as "underlying connection was closed..." when the number of concurrent requests is
                //greater than 2.

                ServicePoint tableServicePoint = ServicePointManager
                    .FindServicePoint(this._storageAccount.TableEndpoint);
                tableServicePoint.ConnectionLimit = 100;
                tableServicePoint.UseNagleAlgorithm = false;
                tableServicePoint.Expect100Continue = false;

                return this.tableClient.GetTableReference(tableName + this.TestString);
            }
            catch (Exception e)
            {
                this.HandleException(e, $"Table {tableName} ");
                throw e;
            }
        }

        public async Task<List<TCustomerEntity>> StreamSameType<TCustomerEntity>(
            string tableName,
            TableQuery<TCustomerEntity> query,
            Action<List<TCustomerEntity>> onLoaded = null,
            int maxEntitiesToFetch = 99)
            where TCustomerEntity : AppTableEntity, new()
        {
            var data = new List<TCustomerEntity>();

            // Retrieve a reference to the table.
            CloudTable table = this.TableClientGetTableReference(tableName);
           await table.CreateIfNotExistsAsync().ConfigureAwait(false);
            TableContinuationToken token = null;
            query = query.Take(maxEntitiesToFetch > 99
                ? 99
                : maxEntitiesToFetch);
            do
            {
                TableQuerySegment<TCustomerEntity> queryResult = await table.ExecuteQuerySegmentedAsync(query, token).ConfigureAwait(false);
                token = queryResult.ContinuationToken;
                List<TCustomerEntity> entities = queryResult.Results;
                data.AddRange(entities);
                onLoaded?.Invoke(entities);
            } while (token != null && data.Count < maxEntitiesToFetch);

            return data;
        }

        public async Task<List<TCustomerEntity>>  StreamSameTypeAllFromTable<TCustomerEntity>(
            string tableName,
            Action<List<TCustomerEntity>> onLoaded = null,
            int maxEntitiesToFetch = 99)
            where TCustomerEntity : AppTableEntity, new()
        {
            return (await this.StreamSameType(tableName, new TableQuery<TCustomerEntity>(), onLoaded, maxEntitiesToFetch).ConfigureAwait(false))
                .ToList();
        }

        public async Task<List<TCustomerEntity>>  StreamSameTypeAllFromPartition<TCustomerEntity>(
            string tableName,
            string partitionKey,
            Action<List<TCustomerEntity>> onLoaded = null,
            int maxEntitiesToFetch = 99)
            where TCustomerEntity : AppTableEntity, new()
        {
            return await this.StreamSameType(tableName,
                new TableQuery<TCustomerEntity>().Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)), onLoaded,
                maxEntitiesToFetch).ConfigureAwait(false);
        }

        public async Task<bool> BulkInsert(string tableName, params AppTableEntity[] list)
        {
            CloudTable table = this.TableClientGetTableReference(tableName);
           await table.CreateIfNotExistsAsync().ConfigureAwait(false);
            TableBatchOperation batchOperation = null;
            int yy = 0;
            list.ToList().ForEach(
                async l =>
                {
                    yy++;
                    if (batchOperation == null)
                        batchOperation = new TableBatchOperation();

                    batchOperation.Insert(l);
                    if (yy % 98 == 0)
                    {
                      await  table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                        batchOperation = null;
                    }
                });
            if (batchOperation != null)
               await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);

            return true;
        }

        public async Task<bool> BulkInsertOrReplace(string tableName, params AppTableEntity[] list)
        {
            CloudTable table = this.TableClientGetTableReference(tableName);
            await table.CreateIfNotExistsAsync().ConfigureAwait(false);
            var batchOperation = new TableBatchOperation();
            int yy = 0;
            list.ToList().ForEach(
                async l =>
                {
                    yy++;
                    if (batchOperation == null)
                        batchOperation = new TableBatchOperation();

                    batchOperation.InsertOrReplace(l);
                    if (yy % 98 == 0)
                    {
                      await  table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                        batchOperation = null;
                    }
                });
            if (batchOperation != null)
               await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);

            return true;
        }

        public async Task<bool> BulkDelete(string tableName, params AppTableEntity[] list)
        {
            CloudTable table = this.TableClientGetTableReference(tableName);
           await table.CreateIfNotExistsAsync().ConfigureAwait(false);
            var batchOperation = new TableBatchOperation();
            int yy = 0;
            list.ToList().ForEach(
                async l =>
                {
                    yy++;
                    if (batchOperation == null)
                        batchOperation = new TableBatchOperation();

                    batchOperation.Delete(l);
                    if (yy % 98 == 0)
                    {
                        await table.ExecuteBatchAsync( batchOperation).ConfigureAwait(false);
                        batchOperation = null;
                    }
                });
            if (batchOperation != null)
               await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
            return true;
        }
      
    }
}