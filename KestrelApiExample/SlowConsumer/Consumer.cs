using System;
using System.Diagnostics;
using System.Threading;
using KestrelApiExample.Stats;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace KestrelApiExample.SlowConsumer
{
    public class Consumer : Threader {
        private string _queueName;
        private readonly int _nrOfMsgsToRead;
        public Stopwatch sw { get; set; }
        public Consumer(string queueName, int nrOfMsgsToRead)
        {
            _queueName = queueName;
            _nrOfMsgsToRead = nrOfMsgsToRead;
        }

        public override void Run()
        {
            int msgs = 0;
            sw = System.Diagnostics.Stopwatch.StartNew();
            while (!_shouldQuit)
            {
                try
                {
                    foreach (var msg in KestrelClusterTest.Manager.Open<string>(_queueName, TimeSpan.FromSeconds(1)))
                    {
                        msgs++;
                        if (msgs == _nrOfMsgsToRead)
                            break;
                    }
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
        }
    }
}