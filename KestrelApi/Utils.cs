using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Primelabs.Twingly.KestrelApi
{
    public class Utils
    {
        public static void Shuffle<T>(IList<T> list)
        {
            // Copied from code at http://en.wikipedia.org/wiki/Fisher-Yates_shuffle
            var r = new Random();
            int n = list.Count;
            while (n > 0)
            {
                n--; // n is now the last pertinent index
                int k = r.Next(n + 1);  // 0 <= k <= n.
                // Simple swap of variables
                T tmp = list[k];
                list[k] = list[n];
                list[n] = tmp;
            }
        }

        /// <summary>
        /// This method parses the given string into an IPEndPoint.
        /// If the string is malformed in some way, or if the host cannot be resolved, this method will throw an exception.
        /// </summary>
        public static IPEndPoint GetEndPoint(string host)
        {
            //Parse port, default to 22133.
            int port = 22133;
            if (host.Contains(":"))
            {
                string[] split = host.Split(new char[] { ':' });
                if (!Int32.TryParse(split[1], out port))
                {
                    throw new ArgumentException("Unable to parse host: " + host);
                }
                host = split[0];
            }

            //Parse host string.
            IPAddress address;
            if (IPAddress.TryParse(host, out address))
            {
                //host string successfully resolved as an IP address.
            }
            else
            {
                //See if we can resolve it as a hostname
                try
                {
                    address = Dns.GetHostEntry(host).AddressList[0];
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to resolve host: " + host, e);
                }
            }

            return new IPEndPoint(address, port);
        }
    }
}
