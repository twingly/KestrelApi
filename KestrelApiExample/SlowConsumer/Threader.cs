using System;
using System.Collections.Generic;
using System.Threading;

namespace KestrelApiExample.SlowConsumer
{
    public class Threader
    {
        protected bool _shouldQuit = false;
        protected Thread _thread;

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
}