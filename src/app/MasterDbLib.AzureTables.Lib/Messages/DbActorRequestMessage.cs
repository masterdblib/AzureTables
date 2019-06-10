//using System;
//using System.Collections.Generic;
//using System.Threading.Tasks;

//namespace MasterDbLib.AzureTables.Lib.Actors.Messages
//{
//    public class DbActorRequestMessage
//    {
//        public DbActorRequestMessage(List<string> transactionId, Func<Task<bool>> operation)
//        {
//            this.Operation = operation;
//            TransactionId = transactionId;
//        }

//        public Func<Task<bool>> Operation { private set; get; }

//        public List<string> TransactionId { private set; get; }
//    }
//}