using System;
using System.Collections.Generic;
using System.Diagnostics;
using Primelabs.Twingly.KestrelApi;

namespace KestrelApiExample.SlowConsumer
{
    public class KestrelSlowConsumerTest : IDisposable
    {
        public static KestrelClusterManager Manager = new KestrelClusterManager("kestrel/cluster1");

        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22134" });
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22133" , "127.0.0.1:22134"});
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "192.168.1.20:22133" });

        //public const string QueueName = "channels.results";
        public const string QueueName = "transient_events";

        public static string ExitMessage = null;

        //static int[] _nrOfThreadsArr = new[] { 1, 2, 4, 8, 16, 32 };
        static int[] _nrOfThreadsArr = new[] { 2,4,8,16,32 };
        private const int _totalNrOfMessages = 100000;

        public void Run()
        {
            // Vi börjar med att skicka 100 000 meddelanden till en kö via en enkel producer.
            // Hur varierar tiden att skicka med antal trådar?
            Manager.FlushAll();
            while (true)
            {
                // TestProducer();
                TestConsumer();
            }

            // Hur varierar tiden att ta emot med antal trådar?
        }

        private const string TheMessage = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
        private void TestConsumer()
        {
            foreach (var nrOfThreads in _nrOfThreadsArr)
            {
                Manager.Flush("test_producer");

                for (int i = 0; i < _totalNrOfMessages; i++)
                {
                    Manager.Send("test_producer", TheMessage);
                }

                var consumers = new List<Consumer>();
                int nrOfMessagesPerThread = _totalNrOfMessages / nrOfThreads;

                for (int i = 0; i < nrOfThreads; i++)
                {
                    var consumer = new Consumer("test_producer", nrOfMessagesPerThread);
                    consumers.Add(consumer);
                }

                var sw = Stopwatch.StartNew();
                foreach (var producer in consumers)
                {
                    producer.Start();
                }

                foreach (var producer in consumers)
                {
                    producer.Stop();
                }
                sw.Stop();

                var message = string.Format("NrOfThreads={0} MsgsPerThread={1} Ms={2}", nrOfThreads, nrOfMessagesPerThread, sw.ElapsedMilliseconds);
                Debug.WriteLine(message);
                Console.WriteLine(message);
            }            
        }

        private void TestProducer()
        {
            foreach (var nrOfThreads in _nrOfThreadsArr)
            {
                Manager.Flush("test_producer");

                var producers = new List<Producer>();
                int nrOfMessagesPerThread = _totalNrOfMessages/nrOfThreads;

                for (int i = 0; i < nrOfThreads; i++)
                {
                    var producer = new Producer("test_producer", nrOfMessagesPerThread, TheMessage);
                    producers.Add(producer);
                }

                var sw = Stopwatch.StartNew();
                foreach (var producer in producers)
                {
                    producer.Start();
                }

                foreach (var producer in producers)
                {
                    producer.Stop();
                }
                sw.Stop();

                var message = string.Format("NrOfThreads={0} MsgsPerThread={1} Ms={2}", nrOfThreads, nrOfMessagesPerThread, sw.ElapsedMilliseconds);
                Debug.WriteLine(message);
                Console.WriteLine(message);
            }
        }

        public void Dispose()
        {
            
        }
    }
}