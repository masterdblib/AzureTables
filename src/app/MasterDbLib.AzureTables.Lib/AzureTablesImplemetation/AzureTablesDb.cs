using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using MasterDbLib.AzureTables.Lib.DataBaseServices.AzureTablesImplemetation.AzureTablesUtility;
using MasterDbLib.Lib.DataBaseServices;
using MasterDbLib.Lib.Messages;
using MasterDbLib.Lib.Utility;
using Newtonsoft.Json;

namespace MasterDbLib.AzureTables.Lib.DataBaseServices.Impl
{
    public class AzureTablesDb : IDb
    {
        
        public AzureTablesDb(string connectionString, string test="")
        {
            StorageService = new AzureCloudTableDataService(connectionString,  test);
        }

        public AzureCloudTableDataService StorageService { get; set; }

        public async Task<T>  GetById<T>(string id) where T : IDbEntity, new()
        {
            var typeOf = typeof(T);
            var tableName = typeOf.Name;
            var partitionKey = id;
            var samePartition = await 
                StorageService.StreamAllDataFromTableWithSamePartitionKey<AppTableEntity>(tableName, partitionKey,
                    null).ConfigureAwait(false);
            return GetDataObject<T>(samePartition);
        }

        private static T GetDataObject<T>(IEnumerable<AppTableEntity> samePartition) where T : IDbEntity, new()
        {
            var data = new T();
            var etags = new List<string>();
            string partitionKey = "";
            samePartition.Select(x =>
            {
                var e = x.ETag;
                partitionKey = x.PartitionKey;
                var propertyName = x.RowKey;
                var propertyType = Type.GetType(x.N);
                var propertyValue = x.V;
                var p = propertyValue==null?null:  Convert.ChangeType(JsonConvert.DeserializeObject(propertyValue), propertyType);
                foreach (PropertyInfo property in
                    typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    var tag = propertyName + ":" + e;
                    if(!etags.Contains(tag))
                        etags.Add(tag);
                    if (propertyName == property.Name)
                        //if (HasProperty<T>( propertyName))
                    {
                        property.SetValue(data, p);
                    }
                }

                return true;
            }).ToList();
            string etag = string.Join(";", etags);
            data.Etag = etag;
            data.Id = partitionKey;
            return data;
        }

        public async Task<bool> Update<T>(T data) where T : IDbEntity, new()
        {
            var entities =  CreateInternal(data, data.Id);
            return  await  StorageService.BulkInsertOrReplaceWithSamePartitionKey(typeof(T).Name, entities.ToArray()).ConfigureAwait(false);
        }
        List<AppTableEntity> CreateInternal<T>(T data, string partitionKey) where T : IDbEntity, new()
        {
            var typeOf = typeof(T);
            var tableName = typeOf.Name;
            var etag = data.Etag?.Split(';')?? new string[0];
            var properties = typeOf.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var entities = new List<AppTableEntity>();

            foreach (PropertyInfo property in properties)
            {
                if (property.Name != "Etag")
                {
                    var entity = new AppTableEntity();
                    var propertyname = property.Name;
                    var propertyType = property.PropertyType.FullName;
                    entity.V = JsonConvert.SerializeObject(property.GetValue(data));
                    //entity.V = property.GetValue(data)?.ToString() ?? (property.PropertyType.IsValueType
                    //               ? Activator.CreateInstance(property.PropertyType)
                    //               : null).ToString();
                    entity.PartitionKey = partitionKey;
                    entity.RowKey = propertyname;
                    entity.N = propertyType;
                    entity.ETag = etag.Where(e => e.Contains(propertyname + ":")).Select(x => x.Split(':')[1]).FirstOrDefault();
                    entities.Add(entity);
                }
            }

            return entities;
        }
        public static bool HasProperty<T>( string propertyName)
        {
            return typeof(T).GetProperty(propertyName) != null;
        }
        public async Task<IEnumerable<T>>  LoadAll<T>() where T : IDbEntity, new()
        {
            var typeOf = typeof(T);
            var tableName = typeOf.Name;
            var samePartition = await 
                StorageService.StreamAllDataFromTable<AppTableEntity>(tableName,null).ConfigureAwait(false);
            var groups = samePartition.GroupBy(x => x.PartitionKey);
            return groups.Select( GetDataObject<T>).ToList();
        }
        
        public async Task<string> CreateNew<T>(T data) where T : IDbEntity, new()
        {
            var id = StorageIdentityGenerator.GenerateId();
            var entities = CreateInternal(data, id);
            await StorageService.BulkInsertOrReplaceWithSamePartitionKey(typeof(T).Name, entities.ToArray()).ConfigureAwait(false);
            return id;
        }
        
        public async Task<bool> DeleteById<T>(string id) where T : IDbEntity, new() 
        {
            var data = await  GetById<T>(id).ConfigureAwait(false);
            var entities = CreateInternal(data, id);
            return await StorageService.BulkDeleteWithSamePartitionKey(typeof(T).Name, entities.ToArray()).ConfigureAwait(false);
        }
    }
}