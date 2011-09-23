using System;
using System.Collections.Generic;
using System.Threading;
using Primelabs.Twingly.KestrelApi;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace KestrelApiExample
{
    public class KestrelClusterTest : IDisposable
    {
        public static KestrelClusterManager Manager = new KestrelClusterManager("kestrel/cluster1");

        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22134" });
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "127.0.0.1:22133" , "127.0.0.1:22134"});
        //public static KestrelClusterManager Manager = new KestrelClusterManager(new string[] { "192.168.1.20:22133" });

        //public const string QueueName = "channels.results";
        public const string QueueName = "transient_events";

        public static string ExitMessage = null;

        public void Run()
        {
            var producers = new List<Producer>();
            var consumers = new List<Consumer>();

            for (int i = 0; i < 1; i++)
            {
                var producer = new Producer(new List<string>(new string[] {"1", "2", "3"}));
                producers.Add(producer);
                producer.Start();
                var consumer = new Consumer(producer);
                consumer.Start();
                consumers.Add(consumer);
            }

            try
            {
                Console.ReadLine();
                ExitMessage = Guid.NewGuid().ToString();
                foreach (var producer in producers)                
                    Manager.Send(QueueName, ExitMessage);
            }
            finally
            {

                foreach (var producer in producers)
                    producer.Stop();
                foreach (var consumer in consumers)
                    consumer.Stop();
            }
            for (int i = 0; i < producers.Count; i++)
            {
                var p = producers[i];
                Console.WriteLine(i);
                ReportStats(p.MessageCount);
                ReportStats(consumers[i].MessageCount);
            }
        }

        public static void ReportStats(Dictionary<string, int> counts)
        {
            foreach (var msg in counts)
                Console.WriteLine(string.Format("{0}={1}", msg.Key, msg.Value));
        }

        public void Dispose()
        {
            
        }
    }

    public class Threader
    {
        protected bool _shouldQuit = false;
        protected Thread _thread;
        public Dictionary<string, int> MessageCount = new Dictionary<string, int>();

        public void Start()
        {
            _thread = new Thread(ThreadProc);
            _thread.Start();
        }

        public void Stop()
        {
            _shouldQuit = true;
            _thread.Join();
        }

        public void ThreadProc()
        {
            Run();
        }

        public virtual void Run()
        {
            Console.WriteLine("run() in threader");
        }

        public static void IncMsg(Dictionary<string,int> dict, string key)
        {
            if (!dict.ContainsKey(key))
            {
                dict[key] = 0;
            }
            dict[key] += 1;            
        }
    }

    public class Consumer: Threader {
        private readonly Producer _producer;

        public Consumer(Producer producer)
        {
            _producer = producer;
        }

        public override void Run()
        {
            int msgs = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (!_shouldQuit)
            {
                try
                {
                    foreach (
                        var msg in
                            KestrelClusterTest.Manager.Open<string>(KestrelClusterTest.QueueName,
                                                                    TimeSpan.FromSeconds(60)))
                    {
                        msgs++;
                        // Console.WriteLine("Got message " + msg.Value);
                        if (KestrelClusterTest.ExitMessage != null && msg.Value == KestrelClusterTest.ExitMessage)
                            break;

                        Threader.IncMsg(MessageCount, msg.Value);
                    }
                    Console.WriteLine("foreach-loop broke, didn't get any message before timeout. ");
                } catch (NoServersAvailable)
                {
                    // all servers down, wait a second and try again..
                    Thread.Sleep(1000);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
            sw.Stop();

            Console.WriteLine("Consumer quitting.");
            Console.WriteLine(string.Format("C;{0};{1};",
                              msgs, msgs / sw.Elapsed.TotalSeconds));
        }
    }

    public class Producer : Threader
    {
        private readonly List<string> _allowedMessages;
        public string ExitMessage = null;

        public Producer(List<string> allowedMessages)
        {
            _allowedMessages = allowedMessages;
        }

        public override void Run()
        {
            int msgs = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (!_shouldQuit)
            {
                try
                {
                    foreach (var msg in _allowedMessages)
                    {
                        KestrelClusterTest.Manager.Send(KestrelClusterTest.QueueName,
                                                        msg);
                                                        // "a message that is not very long... ölajsdflj asöldfj aösldjf ölasjd f" + Guid.NewGuid().ToString());
                        msgs++;
                        IncMsg(MessageCount, msg);
                    }
                    // System.Threading.Thread.Sleep(1000);
                } 
                catch (NoServersAvailable)
                {
                    // all servers down, wait a second and try again..
                    Thread.Sleep(1000);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
            sw.Stop();
            Console.WriteLine(string.Format("P;{0};{1};",
              msgs, msgs / sw.Elapsed.TotalSeconds));
        }
    }
}