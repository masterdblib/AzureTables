//using System.Collections.Generic;
//using MasterDbLib.AzureTables.Lib.DataBaseServices;

//namespace MasterDbLib.AzureTables.Lib.Messages
//{
//    public class DbEntityMessageCompleted<T> where T : IDbEntity
//    {
        
//        public DbEntityMessageCompleted(bool isSuccessful, string message)
//        {
//            IsSuccessful = isSuccessful;
//            Message = message;
//        }
//        public DbEntityMessageCompleted(bool isSuccessful, List<T> entities)
//        {
//            IsSuccessful = isSuccessful;
//            Entities = entities;
//        }
//        public List<T> Entities { get; private set; }
//        public bool IsSuccessful  { get; private set; }
//        public string Message { get; private set; }
//    }
//}