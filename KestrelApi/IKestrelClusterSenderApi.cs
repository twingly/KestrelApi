using System;
using System.Collections.Generic;

namespace Primelabs.Twingly.KestrelApi
{
    public interface IKestrelClusterApi
    {
        // Chooses any server in the cluster and sends the message.
        bool Send(string queueName, object value);
        bool Send(string queueName, object value, TimeSpan expiresIn);
        bool Send(string queueName, object value, DateTime expiresAt);

        IEnumerable<IOpenMessage<T>> Open<T>(string queueName, TimeSpan timeout);
    }
}