using MasterDbLib.AzureTables.Lib;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MasterDbLib.AzureTables.Lib.DataBaseServices;
using MasterDbLib.AzureTables.Lib.DataBaseServices.Impl;
using MasterDbLib.Lib;
using MasterDbLib.Lib.DataBaseServices.InMemoryStorageImplemetation;
using MasterDbLib.Lib.Messages;


namespace MasterDbLib.AzureTables
{
    class Program
    {
        static void Main(string[] args)

        {
           // var dbx = new AzureTablesDb("");
            var dbx = new AzureTablesDb("");

            var db = new  MasterDB(dbx);

            MasterDB.TurnOnEventualConsistencyReloading = true;
            MasterDB.EventualConsistencyReloadingInterval = TimeSpan.FromSeconds(30);
            var iddd=db.CreateNewAsync(new Testatavd(){My1Name2 =DateTime.UtcNow}).Result;

            //while (true)
            //{
            //    System.Threading.Thread.Sleep(5000);
            //    Console.Clear();
            //    Console.WriteLine(db.GetById<Testatav>(iddd)?.My1Name2);
            //}


            var allr2 = db.LoadAll<Testatavd>();
            foreach (var testatav in allr2)
            {
                var tttt = db.DeleteAsync<Testatavd>(testatav).Result;
                Assert.IsTrue(tttt.IsSuccessful);
            }

            Parallel.ForEach(Enumerable.Range(0, 1), (testata) => {
              var yyy=  db.CreateNewAsync(new Testatavd()).Result;

            });

            var id1 = db.CreateNewAsync(new Testatavd()).Result;

            var data1 = db.GetById<Testatavd>(id1);

            var id2 = db.CreateNewAsync(new Testatavd()).Result;
            var data2 = db.GetById<Testatavd>(id2);

            var all = db.LoadAll<Testatavd>();
            var timer = Stopwatch.StartNew();

            Enumerable.Range(0, 1).AsParallel().ForAll(x =>
            {

                Parallel.ForEach(all, (testata) => {

                    var data = db.GetById<Testatavd>(testata.Id);
                    data.MyName = "sam-" + testata.Id + "g" + x;
                    db.UpdateAsync<Testatavd>(data).Wait();
                });
            });
            timer.Stop();

            Console.WriteLine("get and update " + timer.ElapsedMilliseconds);
            var all22e = db.LoadAll<Testatavd>();


            var timer1 = Stopwatch.StartNew();
            Enumerable.Range(0, 1000).AsParallel().ForAll(x =>
            {
                foreach (var testata in all)
                {
                    var data = db.GetById<Testatavd>(testata.Id);
                }
            });
            timer1.Stop();

            Console.WriteLine("Gets 1000 times : " + timer1.ElapsedMilliseconds);
            var timer11 = Stopwatch.StartNew();
            Enumerable.Range(0, 1000).AsParallel().ForAll(x =>
            {

                var all22 = db.LoadAll<Testatavd>();
            });
            timer11.Stop();

            Console.WriteLine("Load all 1000 times : " + timer11.ElapsedMilliseconds);





            Console.WriteLine("Gets 1000 times : " + timer1.ElapsedMilliseconds);
            var timer1w1 = Stopwatch.StartNew();
            Enumerable.Range(0, 1000).AsParallel().ForAll(x =>
            {

                var all22 = db.LoadAll<Testatavd>().ToList();
            });
            timer1w1.Stop();

            Console.WriteLine("Load all 1000 times tolist: " + timer1w1.ElapsedMilliseconds);
            

            Console.WriteLine("Gets 1000 times : " + timer1.ElapsedMilliseconds);
            var timer111 = Stopwatch.StartNew();
            Enumerable.Range(0, 1000).AsParallel().ForAll(x =>
            {

                var all22 = db.LoadAll<Testatavd>().Where(xx => xx.MyName != null).ToList();
            });
            timer111.Stop();

            Console.WriteLine("Query all 1000 times : " + timer111.ElapsedMilliseconds);

            var rr = db.UpdateAsync<Testatavd>(data1).Result;
            Assert.IsFalse(rr.IsSuccessful);

            var data11 = db.GetById<Testatavd>(id1);
            var data111 = db.GetById<Testatavd>(id1);
            data11.MyName = "xxxxxxxxxxx";
            var t11 = db.UpdateAsync<Testatavd>(data11).Result;
            Assert.IsTrue(t11.IsSuccessful);

            data111.MyName = "kkkkkkkk";
            var t111 = db.UpdateAsync<Testatavd>(data111).Result;
            Assert.IsFalse(t111.IsSuccessful);
            //foreach (var testata in all)
            //{
            //    db.DeleteById<Testatav>(testata.Id);
            //}
            var all2 = db.LoadAll<Testatavd>();
            foreach (var testatav in all2)
            {
                var tttt = db.DeleteAsync<Testatavd>(testatav).Result;
                Assert.IsTrue(tttt.IsSuccessful);
            }
        }
    }

    public class Assert
    {
        public static void IsTrue(bool x)
        {
            if (x != true)
            {
                throw  new Exception();
            }
        } public static void IsFalse(bool x)
        {
            if (x == true)
            {
                throw  new Exception();
            }
        }
    }

    [Serializable]
    public class Testatavd : IDbEntity
    {
        public string MyName { set; get; }
        public DateTime MyName2 { set; get; }
        public bool MyName22 { set; get; }
        public int MyName2t2 { set; get; }

        public string MyNa1me { set; get; }
        public DateTime My1Name2 { set; get; }
        public bool MyNam1e22 { set; get; }
        public int MyName12t2 { set; get; }

        public string MyN2a1me { set; get; }
        public DateTime My12Name2 { set; get; }
        public bool MyNam12e22 { set; get; }
        public int MyName212t2 { set; get; }

        public string MyN24a1me { set; get; }
        public DateTime My412Name2 { set; get; }
        public bool MyNam412e22 { set; get; }
        public int MyN4ame212t2 { set; get; }

        public string MyNr24a1me { set; get; }
        public DateTime My41r2Name2 { set; get; }
        public bool MyNam4r12e22 { set; get; }
        public int MyN4amre212t2 { set; get; }

        public string MyNrr24a1me { set; get; }
        public DateTime My41rr2Name2 { set; get; }
        public bool MyNarm4r12e22 { set; get; }
        public int MyrN4amre212t2 { set; get; }
        public string MyNrry24a1me { set; get; }
        public DateTime My41yrr2Name2 { set; get; }
        public bool MyNarm4ry12e22 { set; get; }
        public int MyrN4amrey212t2 { set; get; }
        public string Id { set; get; }
        public string Etag { set; get; }
        public Testatavd()
        {
        }
    }
}
