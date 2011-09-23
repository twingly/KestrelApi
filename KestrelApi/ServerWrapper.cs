using System;

namespace Primelabs.Twingly.KestrelApi
{
    public class ServerWrapper : IDisposable
    {
        private readonly ServerPool _pool;
        public KestrelClient Client { get; set; }

        public ServerWrapper(ServerPool pool, KestrelClient client)
        {
            _pool = pool;
            Client = client;
        }

        public void Dispose()
        {
            _pool.Release(this.Client);
        }
    }
}