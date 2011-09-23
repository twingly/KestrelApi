using System;
using System.Collections.Generic;
using System.Net;

namespace Primelabs.Twingly.KestrelApi.Configuration
{
    public class KestrelClusterConfiguration : IKestrelClusterConfiguration
    {
        public KestrelClusterConfiguration(IList<IPEndPoint> endpoints)
        {
            SendReceiveTimeout = 10000;

            MinConnections = 10;
            MaxConnections = 30;
            DeadHostRetryInterval = TimeSpan.FromSeconds(60);

            MaxNumberOfMessageInBatch = 100;
            MinServerTimeout = TimeSpan.FromSeconds(1);

            Servers = endpoints;
        }

        public int SendReceiveTimeout { get; private set; }
        public uint MinConnections { get; private set; }
        public uint MaxConnections { get; private set; }
        public TimeSpan DeadHostRetryInterval { get; private set; }
        public IEnumerable<IPEndPoint> Servers { get; private set; }
        public uint MaxNumberOfMessageInBatch { get; private set; }
        public TimeSpan MinServerTimeout { get; private set; }
    }
}