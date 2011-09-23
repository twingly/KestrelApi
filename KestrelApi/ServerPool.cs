using System;
using System.Collections.Generic;
using System.Net;
using Primelabs.Twingly.KestrelApi.Configuration;

namespace Primelabs.Twingly.KestrelApi
{
    public class ServerPool : IDisposable
    {
        private readonly IKestrelClusterConfiguration _configuration;

        protected class ServerInfo
        {
            public IPEndPoint Endpoint { get; set; }
            public bool IsDead { get; set; }
            public DateTime NextRetry { get; set; }

            public Queue<KestrelClient> Queue { get; set; }
        }

        private bool _isDisposed;
        protected Dictionary<string, ServerInfo> _poolPerIp;

        public ServerPool(IKestrelClusterConfiguration configuration)
        {
            _configuration = configuration;
            _poolPerIp = new Dictionary<string, ServerInfo>();

            foreach (var endpoint in _configuration.Servers)
            {
                var serverInfo = new ServerInfo
                                     {
                                         Endpoint = endpoint,
                                         IsDead = false,
                                         NextRetry = DateTime.Now,
                                         Queue = new Queue<KestrelClient>(),
                                     };

                _poolPerIp[EndPointHashValue(endpoint)] = serverInfo;
            }
        }

        protected string EndPointHashValue(IPEndPoint endpoint)
        {
            return endpoint.ToString();
        }

        protected ServerInfo GetServerInfoByEndPoint(IPEndPoint endpoint)
        {
            var key = EndPointHashValue(endpoint);
            ServerInfo serverInfo;
            if (! _poolPerIp.TryGetValue(key, out serverInfo))
            {
                throw new ArgumentException("Endpoint not registered in pool!", "endpoint");
            }
            return serverInfo;
        }

        protected KestrelClient CreateServer(IPEndPoint endpoint)
        {
            var socket = new WrappedSocket(endpoint, _configuration.SendReceiveTimeout);
            var protocol = new KestrelProtocol(socket);
            var server = new KestrelClient(protocol);
            return server;
        }

        public KestrelClient Acquire(IPEndPoint endpoint)
        {
            if (_isDisposed)
                throw new InvalidOperationException("Cannot Acquire, as the pool has been disposed of.");

            var serverInfo = GetServerInfoByEndPoint(endpoint);
            lock (serverInfo)
            {
                var queue = serverInfo.Queue;
                while (queue.Count > 0)
                {
                    var server = queue.Dequeue();
                    // Check if dead or tainted, just a safety-net, as they should
                    // never have been in the pool in the first place..
                    if (server.IsTainted || ! server.IsAlive)
                    {
                        using (server)
                        {
                            server.Disconnect();
                        }
                    }
                    else
                    {
                        return server;
                    }
                }

                // no connection found in pool, has the server's been dead long enough 
                // to try create a  new one?
                if (serverInfo.IsDead)
                {
                    if (DateTime.Now > serverInfo.NextRetry)
                    {
                        serverInfo.IsDead = false;
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            // try creating the socket.
            KestrelClient newServer = null;
            try
            {
                newServer = CreateServer(endpoint);
                newServer.Connect();
                return newServer;
            } catch (Exception)
            {
                if (newServer != null)
                    newServer.Dispose();

                lock (serverInfo)
                {
                    serverInfo.IsDead = true;
                    // exponential backoff here?
                    serverInfo.NextRetry = DateTime.Now + _configuration.DeadHostRetryInterval;
                }
                return null;
            }
        }

        public void Release(KestrelClient client)
        {
            if (client.IsTainted || ! client.IsAlive || _isDisposed)
            {
                using (client)
                {
                    client.Disconnect();
                }
                return;
            }

            // client should be ok, return it to the pool
            var endpoint = client.Protocol.Socket.EndPoint;
            var serverInfo = GetServerInfoByEndPoint(endpoint);
            lock (serverInfo)
            {
                if (serverInfo.Queue.Count > this._configuration.MaxConnections)
                {
                    using (client)
                    {
                        client.Disconnect();
                    }
                }
                else
                {
                    serverInfo.Queue.Enqueue(client);
                }
            }
        }

        public void Dispose()
        {
            _isDisposed = true;
            foreach (var key in _poolPerIp.Keys)
            {
                var serverInfo = _poolPerIp[key];
                lock (serverInfo)
                {
                    while (serverInfo.Queue.Count > 0)
                    {
                        var client = serverInfo.Queue.Dequeue();
                        using (client)
                            client.Disconnect();
                    }
                }
            }
        }
    }
}