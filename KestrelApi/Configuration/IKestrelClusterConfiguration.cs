using System;
using System.Collections.Generic;
using System.Net;

namespace Primelabs.Twingly.KestrelApi.Configuration
{
    public interface IKestrelClusterConfiguration
    {
        /// <summary>
        /// Socket send/receive timeout (milliseconds)
        /// </summary>
        int SendReceiveTimeout { get; }

        /// <summary>
        /// Minimum number of connections to each server in the pool
        /// </summary>
        uint MinConnections { get; }

        /// <summary>
        /// Maximum number of connections to each server in the pool
        /// </summary>
        uint MaxConnections { get; }

        /// <summary>
        /// Time to wait before retrying connecting to a host that was determined to be dead.
        /// </summary>
        TimeSpan DeadHostRetryInterval { get; }

        /// <summary>
        /// A list of the servers available to the pool.
        /// </summary>
        IEnumerable<IPEndPoint> Servers { get; }

        /// <summary>
        /// Maximum number of messages to be received in batch.
        /// </summary>
        uint MaxNumberOfMessageInBatch { get; }

        /// <summary>
        /// Minimum timeout to send to each server when using GET /open
        /// </summary>
        TimeSpan MinServerTimeout { get; }

    }
}