using System;
using System.Collections.Generic;
using System.Threading;
using Primelabs.Twingly.KestrelApi;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace KestrelApiExample.Stats
{
    public class StatsExample
    {
        public static KestrelClusterManager Manager = new KestrelClusterManager("kestrel/cluster1");

        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22134" });
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22133" , "127.0.0.1:22134"});
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "192.168.1.20:22133" });

        public void Run()
        {
            var stats = Manager.GetStats();

        }
    }
}