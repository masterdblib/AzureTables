using Microsoft.WindowsAzure.Storage.Table;

namespace MasterDbLib.AzureTables.Lib.DataBaseServices.AzureTablesImplemetation.AzureTablesUtility
{ public class AppTableEntity : TableEntity
        { 
            public string N { get; set; }
            public string V { get; set; }
        }
  
}