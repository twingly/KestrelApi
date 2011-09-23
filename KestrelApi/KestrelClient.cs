using System;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace Primelabs.Twingly.KestrelApi
{
    public class KestrelClient : IKestrelServerApi, IDisposable
    {
        public KestrelProtocol Protocol { get; private set; }

        private bool _tainted;
        public bool IsTainted { get { return _tainted; } }
        public bool IsAlive { get { return Protocol.IsAlive; } }

        public KestrelClient(KestrelProtocol protocol)
        {
            Protocol = protocol;
            _tainted = false;
        }

        public virtual void Connect()
        {
            Protocol.Connect();
        }

        public virtual void Disconnect()
        {
            Protocol.Disconnect();
        }

        protected void CheckIfTainted()
        {
            if (_tainted)
            {
                throw new KestrelApiException("Client is tainted, dispose of it and create a new one!");
            }
        }

        protected delegate T GenericReturningAction<T>();
        protected delegate T GenericReturningAction<T, T2>(out T2 obj);

        protected T CheckTaintAndPerform<T>(GenericReturningAction<T> action)
        {
            CheckIfTainted();
            try
            {
                return action();
            } catch (Exception)
            {
                _tainted = true;
                throw;
            }
        }

        public bool Send(string queueName, object value)
        {
            return CheckTaintAndPerform(() => Protocol.Set(queueName, value, 0));
        }

        public bool Send(string queueName, object value, TimeSpan expiresIn)
        {
            return CheckTaintAndPerform(() =>  Protocol.Set(queueName, value, expiresIn));
        }

        public bool Send(string queueName, object value, DateTime expiresAt)
        {
            return CheckTaintAndPerform(() =>  Protocol.Set(queueName, value, expiresAt));
        }

        public bool Peek<T>(string queueName, out T obj)
        {
            return GetItem(queueName + "/peek", null, out obj);
        }

        public bool Get<T>(string queueName, out T value, TimeSpan? timeout)
        {
            return GetItem(queueName, timeout, out value);
        }

        private bool GetItem<T>(string key, TimeSpan? timeout, out T value)
        {
            if (timeout.HasValue)
                key += ("/t=" + (long) timeout.Value.TotalMilliseconds);
            
            value = default(T);

            object val = null;

            if (CheckTaintAndPerform(() => Protocol.Get(key, out val)))
            {
                value = (T) val;
                return true;
            }
            return false;
        }

        public bool Open<T>(string queueName, out T value, TimeSpan? timeout, bool close)
        {
            var key = queueName;
            if (close)
                key += "/close";

            key += "/open";

            return GetItem(key, timeout, out value);
        }

        public void Close(string queueName)
        {
            // This doesn't return anythinig. Kestrel returns an empty answer
            // when the transaction was closed. If there is no open transaction, it throws an ERROR
            object tmp;
            CheckTaintAndPerform(() => Protocol.Get(queueName + "/close", out tmp));
        }

        public void Abort(string queueName)
        {
            // This doesn't return anythinig. Kestrel returns an empty answer
            // when the transaction was aborted. If there is no open transaction, it throws an ERROR
            object tmp;
            CheckTaintAndPerform(() => Protocol.Get(queueName + "/abort", out tmp));
        }

        public bool FlushAll()
        {
            return CheckTaintAndPerform(() => Protocol.FlushAll());
        }

        public bool Flush(string queueName)
        {
            return CheckTaintAndPerform(() => Protocol.Flush(queueName));
        }

        public Stats GetStats()
        {
            return CheckTaintAndPerform(() => Protocol.GetStats());
        }

        public void Dispose()
        {
            Disconnect();
        }
    }
}