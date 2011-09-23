using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Net;

namespace Primelabs.Twingly.KestrelApi.Configuration
{
    public sealed class KestrelClusterConfigurationSection : ConfigurationSection, IKestrelClusterConfiguration
    {
        [ConfigurationProperty("sendReceiveTimeout", IsRequired = false, DefaultValue = 10000), IntegerValidator(MinValue = 0, MaxValue = 1000000)]
        public int SendReceiveTimeout
        {
            get { return (int)base["sendReceiveTimeout"]; }
            set { base["sendReceiveTimeout"] = value; }
        }

        [ConfigurationProperty("minConnections", IsRequired = false, DefaultValue = 10), IntegerValidator(MinValue = 0, MaxValue = 10000)]
        public int MinConnections
        {
            get { return (int)base["minConnections"]; }
            set { base["minConnections"] = value; }
        }

        uint IKestrelClusterConfiguration.MinConnections
        {
            get
            {
                return (uint)(int)base["minConnections"];
            }
        }

        [ConfigurationProperty("maxConnections", IsRequired = false, DefaultValue = 30), IntegerValidator(MinValue = 0, MaxValue = 10000)]
        public int MaxConnections
        {
            get { return (int)base["maxConnections"]; }
            set { base["maxConnections"] = value; }
        }

        uint IKestrelClusterConfiguration.MaxConnections
        {
            get
            {
                return (uint)(int)base["maxConnections"];
            }
        }

        [ConfigurationProperty("maxNumberOfMessageInBatch", IsRequired = false, DefaultValue = 100), IntegerValidator(MinValue = 1, MaxValue = 100000)]
        public int MaxNumberOfMessageInBatch
        {
            get { return (int)base["maxNumberOfMessageInBatch"]; }
            set { base["maxNumberOfMessageInBatch"] = value; }
        }

        uint IKestrelClusterConfiguration.MaxNumberOfMessageInBatch
        {
            get
            {
                return (uint)(int)base["maxNumberOfMessageInBatch"];
            }
        }

        [ConfigurationProperty("deadHostRetryInterval", IsRequired = false, DefaultValue = "00:01:00"), PositiveTimeSpanValidator, TypeConverter(typeof(InfiniteTimeSpanConverter))]
        public TimeSpan DeadHostRetryInterval
        {
            get { return (TimeSpan)base["deadHostRetryInterval"]; }
            set { base["deadHostRetryInterval"] = value; }
        }

        [ConfigurationProperty("minServerTimeout", IsRequired = false, DefaultValue = "00:00:01"), PositiveTimeSpanValidator, TypeConverter(typeof(InfiniteTimeSpanConverter))]
        public TimeSpan MinServerTimeout
        {
            get { return (TimeSpan)base["minServerTimeout"]; }
            set { base["minServerTimeout"] = value; }
        }

        public IEnumerable<IPEndPoint> Servers
        {
            get
            {
                var retval = new List<IPEndPoint>();
                foreach (var rawServer in this.ServersRaw)
                {
                    var endPoint = Utils.GetEndPoint(rawServer);
                    retval.Add(endPoint);
                }
                return retval;
            }
        }

        /// <summary>
        /// Returns a collection of Memcached servers which can be used by the client.
        /// </summary>
        [ConfigurationProperty("servers", IsRequired = true), TypeConverter(typeof(CommaDelimitedStringCollectionConverter))]
        public CommaDelimitedStringCollection ServersRaw
        {
            get { return (CommaDelimitedStringCollection)base["servers"]; }
        }

    }
}