using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MasterDbLib.AzureTables.Lib.DataBaseServices.AzureTablesImplemetation.AzureTablesUtility
{
    public interface ITableStorageDataService
    {
        string TestString { set; get; }


        Task<bool> BulkInsertWithSamePartitionKey(string storageAccess, AppTableEntity[] rows);

        Task<bool> BulkDeleteWithSamePartitionKey(string storageAccess, AppTableEntity[] rows);

        Task<bool> BulkInsertOrReplaceWithSamePartitionKey(string storageAccess, AppTableEntity[] rows);

        //get list
       Task<List<T>>  StreamAllDataFromTable<T>(string storageAccess, Action<List<T>> action)
            where T : AppTableEntity, new();
        //get object
      Task< List<T>> StreamAllDataFromTableWithSamePartitionKey<T>(string storageAccess, string partitionKey,
            Action<List<T>> o)
            where T : AppTableEntity, new();
    }
}