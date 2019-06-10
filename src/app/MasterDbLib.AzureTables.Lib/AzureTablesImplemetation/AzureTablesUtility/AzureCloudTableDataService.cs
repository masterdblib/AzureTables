/*
The Golden Rules
===================
You can perform updates, deletes, and inserts in the same single batch operation.
A single batch operation can include up to 100 entities.
All entities in a single batch operation must have the same partition key.
While it is possible to perform a query as a batch operation, it must be the only operation in the batch.
Tables don’t enforce a schema on entities, which means a single table can contain entities that have different sets of properties. An account can contain many tables, the size of which is only limited by the 100TB storage account limit.
An entity is a set of properties, similar to a database row. An entity can be up to 1MB in size.
A property is a name-value pair. Each entity can include up to 252 properties to store data. Each entity also has 3 system properties that specify a partition key, a row key, and a timestamp. Entities with the same partition key can be queried more quickly, and inserted/updated in atomic operations. An entity’s row key is its unique identifier within a partition.
A property value may be up to 64 KB in size.
By default a property is created as type String, unless you specify a different type
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MasterDbLib.AzureTables.Lib.DataBaseServices.AzureTablesImplemetation.AzureTablesUtility
{
    public class AzureCloudTableDataService : ITableStorageDataService
    {
        public AzureCloudTableDataService(string connectionString, string test)
        {
            this.TestString = test;
            this.AzureTableStorageUtility = new AzureAppTableStorageUtility(connectionString, test);
        }

        private AzureAppTableStorageUtility AzureTableStorageUtility { get; }

        public string TestString { get; set; }

        public Task<bool> BulkInsertWithSamePartitionKey(string storageAccess, AppTableEntity[] rows)
        {
          return  this.AzureTableStorageUtility.BulkInsert(storageAccess, rows);
        }

        public Task<bool> BulkDeleteWithSamePartitionKey(string storageAccess, AppTableEntity[] rows)
        {
          return  this.AzureTableStorageUtility.BulkDelete(storageAccess, rows);
        }

        public Task<bool> BulkInsertOrReplaceWithSamePartitionKey(string storageAccess, AppTableEntity[] rows)
        {
          return  this.AzureTableStorageUtility.BulkInsertOrReplace(storageAccess, rows);
        }

        public async Task<List<T>>  StreamAllDataFromTable<T>(string storageAccess, Action<List<T>> action)
            where T : AppTableEntity, new()
        {
            return await AzureTableStorageUtility.StreamSameTypeAllFromTable(
                storageAccess,
                action,
                int.MaxValue).ConfigureAwait(false);
        }

        public async Task<List<T>>  StreamAllDataFromTableWithSamePartitionKey<T>(string storageAccess, string partitionKey, Action<List<T>> o)
            where T : AppTableEntity, new()
        {
            return (await this.AzureTableStorageUtility.StreamSameTypeAllFromPartition(storageAccess, partitionKey, o).ConfigureAwait(false)).Select(x => (T)x).ToList();
        }
    }
}