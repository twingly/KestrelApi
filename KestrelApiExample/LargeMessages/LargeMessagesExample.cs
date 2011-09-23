using System;
using System.Collections.Generic;
using System.Threading;
using Primelabs.Twingly.KestrelApi;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace KestrelApiExample.LargeMessages
{
    public class LargeMessagesExample
    {
        public static KestrelClusterManager Manager = new KestrelClusterManager("kestrel/cluster1");

        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22134" });
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22133" , "127.0.0.1:22134"});
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "192.168.1.20:22133" });

        public void Run()
        {
            var rand = new Random();
            byte[] arr = new byte[2500000];
            for (int i = 0; i < arr.Length; i++)
            {
                arr[i] = (byte)rand.Next(255);
            }
            var success = false;
            int fails = 0;
            while (!success)
            {
                try
                {
                    Manager.Send("test_producer", arr);
                    success = true;
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    fails++;
                }
            }
            Console.WriteLine("fails=" + fails);
        }
    }
}