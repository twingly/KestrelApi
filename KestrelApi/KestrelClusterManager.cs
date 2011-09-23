using System;
using System.Collections.Generic;
using System.Net;
using Primelabs.Twingly.KestrelApi.Configuration;
using System.Linq;
using Primelabs.Twingly.KestrelApi.Exceptions;
using System.Configuration;

namespace Primelabs.Twingly.KestrelApi
{
    /*
    public interface IEndPointSelectionAlgorithm
    {
        IEnumerable<EndPoint> GetSuitableServer(KestrelOperation operation, string queueName);
    }

    public class RandomizedEndpointSelection : IEndPointSelectionAlgorithm
    {
        private List<EndPoint> _endpoints;
        public RandomizedEndpointSelection(IEnumerable<EndPoint> endpoints)
        {
            _endpoints = endpoints.ToList();
        }

        public IEnumerable<EndPoint> GetSuitableServer(KestrelOperation operation, string queueName)
        {
            for (int i = 0; i < _endpoints.Count; i++)
            {
                yield return null;
            }
        }
    }
    */

    public class KestrelClusterManager : IKestrelClusterApi, IDisposable
    {
        protected List<IPEndPoint> _serverEndpoints;
        protected ServerPool _serverPool;
        IKestrelClusterConfiguration _configuration;

        public KestrelClusterManager(string[] adresses)
        {
            _serverEndpoints = new List<IPEndPoint>();
            foreach (var host in adresses)
                _serverEndpoints.Add(Utils.GetEndPoint(host));

            _configuration = new KestrelClusterConfiguration(_serverEndpoints);
            _serverPool = new ServerPool(_configuration);
        }

		public KestrelClusterManager(string sectionName)
		{
            KestrelClusterConfigurationSection section = (KestrelClusterConfigurationSection)ConfigurationManager.GetSection(sectionName);
			if (section == null)
				throw new ConfigurationErrorsException("Section " + sectionName + " is not found.");

            _configuration = section;
            _serverPool = new ServerPool(_configuration);

		    _serverEndpoints = _configuration.Servers.ToList();
        }

        protected IEnumerable<ServerWrapper> IterateServers(bool randomOrder, bool dispose)
        {
            // TODO: Some kind of smarter selection wrt to multiple readers ? 
            // Ideally we would like all readers spread out as evenly as possible
            // throughout the servers among the cluster

            var list = _serverEndpoints.ToList();
            if (randomOrder)
                Utils.Shuffle(list);

            foreach (var endpoint in list)
            {
                var client = _serverPool.Acquire(endpoint);
                if (client != null)
                {
                    if (dispose)
                    {
                        using (var retval = new ServerWrapper(_serverPool, client))
                        {
                            yield return retval;
                        }
                    }
                    else
                    {
                        yield return new ServerWrapper(_serverPool, client);
                    }
                }
            }
        }

        protected ServerWrapper GetServer()
        {
            foreach (var server in IterateServers(true, false))
                return server;
            throw new NoServersAvailable("Tried " + _serverEndpoints.Count + " endpoints, all found dead!");
        }

        private TimeSpan GetTimeoutPerServer(TimeSpan userTimeout)
        {
            double tmp = userTimeout.TotalSeconds/_serverEndpoints.Count;

            if (tmp > _configuration.MinServerTimeout.TotalSeconds)
                return _configuration.MinServerTimeout;
            else
                return TimeSpan.FromSeconds(tmp);
        }

        public void Flush(string queueName)
        {
            using (var wrapper = GetServer())
            {
                wrapper.Client.Flush(queueName);
            }
        }

        public void FlushAll()
        {
            using (var wrapper = GetServer())
            {
                wrapper.Client.FlushAll();
            }
        }

        public Dictionary<IPEndPoint, Stats> GetStats()
        {
            var retval = new Dictionary<IPEndPoint, Stats>();
            foreach (var wrapper in IterateServers(false, true))
            {
                try
                {
                    retval[wrapper.Client.Protocol.Socket.EndPoint] = wrapper.Client.GetStats();
                } 
                catch (Exception)
                {
                    // Empty on purpose, we just keep on iterating, trying to collect something.
                }
            }
            return retval;
        }

        protected bool IterateServersUntilSendSucceeds(Func<ServerWrapper, bool> action)
        {
            foreach (var wrapper in IterateServers(true, true))
            {
                bool retval;
                try
                {
                    retval = action(wrapper);
                } 
                catch (Exception)
                {
                    continue;
                }
                if (retval)
                    return true;
            }
            throw new NoServersAvailable("Tried " + _serverEndpoints.Count + " endpoints, no server accepted the message!");
        }

        public bool Send(string queueName, object value)
        {
            return IterateServersUntilSendSucceeds(x => x.Client.Send(queueName, value));
        }

        public bool Send(string queueName, object value, TimeSpan expiresIn)
        {
            return IterateServersUntilSendSucceeds(x => x.Client.Send(queueName, value, expiresIn));
        }

        public bool Send(string queueName, object value, DateTime expiresAt)
        {
            return IterateServersUntilSendSucceeds(x => x.Client.Send(queueName, value, expiresAt));
        }

        public IEnumerable<IOpenMessage<T>> Open<T>(string queueName, TimeSpan timeout)
        {
            return Open<T>(queueName, timeout, () => new OpenMessage<T>());
        }

        public IEnumerable<IOpenMessage<T>> Open<T>(string queueName, TimeSpan timeout, OpenMessageFactoryDelegate<T> factory)
        {
            bool shouldQuit = false;

            DateTime timeoutAt = DateTime.Now.AddSeconds(timeout.TotalSeconds);
            bool hasOpenUnacknowledgeMessage = false;
            while (!shouldQuit)
            {
                using (var server = GetServer())
                {
                    uint messageThisBatch = 0;
                    while (!shouldQuit)
                    {
                        IOpenMessage<T> msg = factory();
                        T value = default(T);
                        bool userBrokeIteration = true;

                        TimeSpan thisRoundTimeout = timeoutAt - DateTime.Now;
                        if (thisRoundTimeout.TotalSeconds < 0)
                        {
                            // We got a real timeout, ack any outstanding message, and make sure we quit
                            if (hasOpenUnacknowledgeMessage)
                            {
                                server.Client.Close(queueName);
                            }
                            shouldQuit = true;
                            break;
                        }

                        // As we ack the message inside the Open below
                        hasOpenUnacknowledgeMessage = false;

                        if (! server.Client.Open<T>(queueName, out value, GetTimeoutPerServer(thisRoundTimeout), true)) {
                            // we got a timeout. break to choose a new server (or timeout), and go again
                            break;
                        }
                        // we need to keep track of this here, in order to close any outstanding message in case
                        // we timeout next time.
                        hasOpenUnacknowledgeMessage = true;

                        try
                        {
                            // We successfully got a message, prepare message and give it to the caller.
                            msg.Value = value;
                            msg.Close = true;
                            yield return msg;
                            
                            userBrokeIteration = false;
                            messageThisBatch += 1;
                            timeoutAt = DateTime.Now + timeout;
                        }
                        finally
                        {
                            if (!msg.Close)
                            {
                                server.Client.Abort(queueName);
                            }
                            else if (userBrokeIteration || BreakDueToBatchSize(messageThisBatch))
                            {
                                server.Client.Close(queueName);
                            }
                            shouldQuit = userBrokeIteration;
                        }

                        // User didn't break the iteration
                        // If we have read enough message this batch, we should switch server.
                        // otherwise, just continue on the same server
                        if (BreakDueToBatchSize(messageThisBatch))
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Checks if we should switch server due to batch size being exhausted..
        /// </summary>
        /// <param name="messageThisBatch"></param>
        /// <returns></returns>
        protected bool BreakDueToBatchSize(uint messageThisBatch)
        {
            return (_configuration.MaxNumberOfMessageInBatch > 0 && messageThisBatch >= _configuration.MaxNumberOfMessageInBatch);
        }
        
        void IDisposable.Dispose()
        {
            // foreach server, close connections a.s.o...
            _serverPool.Dispose();
        }
    }
}