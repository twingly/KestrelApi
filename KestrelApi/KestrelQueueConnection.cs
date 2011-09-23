using System;

namespace Primelabs.Twingly.KestrelApi
{
    public class KestrelQueueConnection
    {
        public IKestrelClusterApi Cluster { get; set; }
        public string QueueName { get; set; }
        public TimeSpan ReceiveTimeout { get; set; }

        public KestrelQueueConnection()
        {
            ReceiveTimeout = TimeSpan.FromSeconds(10);
        }

        public KestrelQueueConnection(IKestrelClusterApi cluster)
            : this()
        {
            Cluster = cluster;
        }

        public KestrelQueueConnection(IKestrelClusterApi cluster, TimeSpan receiveTimeout, string queueName)
        {
            Cluster = cluster;
            ReceiveTimeout = receiveTimeout;
            QueueName = queueName;
        }

        public void ParseMetadata(string raw)
        {
            // implement parsing of something that can be used in appsettings.
            var data = raw.Split(new char[] { ',' });
            QueueName = data[0];
            if (data.Length > 1)
            {
                ReceiveTimeout = TimeSpan.FromSeconds(double.Parse(data[1]));
            }
        }
    }
}