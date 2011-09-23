using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using KestrelApiExample.Stats;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace KestrelApiExample.SlowConsumer
{
    public class Producer : Threader
    {
        private string _queueName;
        private readonly int _nrOfMessages;
        private string _msg;
        public Stopwatch Sw { get; set; }

        public Producer(string queueName, int nrOfMessages, string msg)
        {
            _queueName = queueName;
            _msg = msg;
            _nrOfMessages = nrOfMessages;
        }

        public override void Run()
        {
            int msgs = 0;
            Sw = System.Diagnostics.Stopwatch.StartNew();
            while (msgs < _nrOfMessages)
            {
                try
                {
                    KestrelClusterTest.Manager.Send(_queueName, _msg);
                    msgs++;
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
            Sw.Stop();
        }
    }
}