//Copyright (c) 2007-2008 Henrik Schröder, Oliver Kofoed Pedersen

//Permission is hereby granted, free of charge, to any person
//obtaining a copy of this software and associated documentation
//files (the "Software"), to deal in the Software without
//restriction, including without limitation the rights to use,
//copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the
//Software is furnished to do so, subject to the following
//conditions:

//The above copyright notice and this permission notice shall be
//included in all copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//OTHER DEALINGS IN THE SOFTWARE.
//
// Code taken/adapted from BeIT Memcached library.
//

using System;
using System.Collections.Specialized;
using System.Configuration;
using System.Globalization;
using System.Text;
using Primelabs.Twingly.KestrelApi;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace Primelabs.Twingly.KestrelApi
{
    /// <summary>
    /// Memcached client main class.
    /// Use the static methods Setup and GetInstance to setup and get an instance of the client for use.
    /// </summary>
    public class KestrelProtocol
    {
        private readonly WrappedSocket _socket;

        /// <summary>
        /// The send receive timeout is used to determine how long the client should wait for data to be sent 
        /// and received from the server, specified in milliseconds. The default value is 2000.
        /// </summary>
        public int SendReceiveTimeout { get; set; }

        /// <summary>
        /// If an object being stored is larger in bytes than the compression threshold, it will internally be compressed before begin stored,
        /// and it will transparently be decompressed when retrieved. Only strings, byte arrays and objects can be compressed.
        /// The default value is 1024 * 128 = 128kb
        /// </summary>
        public uint CompressionThreshold { get; set; }

        public KestrelProtocol(WrappedSocket socket)
        {
            CompressionThreshold = 1024*128;
            _socket = socket;
        }

        #region Helpers
        /// <summary>
        /// Private key-checking method.
        /// Throws an exception if the key does not conform to memcached protocol requirements:
        /// It may not contain whitespace, it may not be null or empty, and it may not be longer than 250 characters.
        /// </summary>
        /// <param name="key">The key to check.</param>
        private void checkKey(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("Key may not be null.");
            }
            if (key.Length == 0)
            {
                throw new ArgumentException("Key may not be empty.");
            }
            if (key.Length > 250)
            {
                throw new ArgumentException("Key may not be longer than 250 characters.");
            }
            foreach (char c in key)
            {
                if (c <= 32)
                {
                    throw new ArgumentException("Key may not contain whitespace or control characters.");
                }
            }
        }

        //Private Unix-time converter
        private static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static int getUnixTime(DateTime datetime)
        {
            return (int)(datetime.ToUniversalTime() - epoch).TotalSeconds;
        }

        protected static byte[] ConvertToLittleEndian(ushort value)
        {
            byte[] arr = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(arr);
            }

            return arr;
        }

        protected static ushort ConvertFromLittleEndian(byte[] raw)
        {
            if (raw == null || raw.Length != 2)
                throw new ArgumentException("Array is null or wrong length!", "raw");

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(raw);
            }

            var retval = BitConverter.ToUInt16(raw, 0);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(raw);
            }

            return retval;
        }

        #endregion

        public bool IsAlive { get { return Socket.IsAlive; } }

        public WrappedSocket Socket
        {
            get { return _socket; }
        }

        public virtual void Connect()
        {
            Socket.Connect();
        }

        public virtual void Disconnect()
        {
            Socket.Close();
        }

        #region Set, Add, and Replace.
        /// <summary>
        /// This method corresponds to the "set" command in the memcached protocol. 
        /// It will unconditionally set the given key to the given value.
        /// Using the overloads it is possible to specify an expiry time, either relative as a TimeSpan or 
        /// absolute as a DateTime. It is also possible to specify a custom hash to override server selection.
        /// </summary>
        public bool Set(string key, object value, TimeSpan expiry) { return Set(key, value, (int)expiry.TotalSeconds); }
        public bool Set(string key, object value, DateTime expiry) { return Set(key, value, getUnixTime(expiry)); }
        //Private common store method.
        public bool Set(string key, object value, int expiry)
        {
            checkKey(key);

            SerializedType type;
            byte[] bytes;

            //Serialize object efficiently, store the datatype marker in the flags property.
            try
            {
                bytes = Serializer.Serialize(value, out type, CompressionThreshold);
            }
            catch (Exception e)
            {
                //If serialization fails, return false;
                throw new KestrelApiException("Error serializing value!", e);
            }

            var flagBytes = ConvertToLittleEndian((ushort) type);
            var totalDataLength = flagBytes.Length + bytes.Length;
            //Create commandline
            string commandline = "SET" + " " + key + " " + (ushort)type + " " + expiry + " " + totalDataLength + "\r\n";

            //Write commandline and serialized object.
            Socket.Write(commandline);
            Socket.Write(flagBytes);
            Socket.Write(bytes);
            Socket.Write("\r\n");
            Socket.Flush();

            var response = Socket.ReadResponse();

            if (response.StartsWith("STORED"))
                return true;
            else if (response.StartsWith("NOT_STORED"))
                return false;
            else
                throw new KestrelApiException("Unexpected reply from kestrel: Expected STORED/NOT_STORED got " + response);
        }

        #endregion

        #region Get
        public bool Get(string key, out object value)
        {
            checkKey(key);

            Socket.WriteAndFlush("GET " + key + "\r\n");

            object tmpValue;
            ulong unique;
            if (ReadValue(Socket, out tmpValue, out key, out unique))
            {
                value = tmpValue;
                var response = Socket.ReadLine(); // Read the trailing END.
                if (response != "END")
                    throw new KestrelApiException("Get failed: Expected string END but got " + response);

                return true;
            }

            value = null;
            return false;
        }

        //Private method for reading results of the "get" command.
        private bool ReadValue(WrappedSocket socket, out object value, out string key, out ulong unique)
        {
            string response = socket.ReadResponse();
            string[] parts = response.Split(' '); //Result line from server: "VALUE <key> <flags> <bytes> <cas unique>"
            if (parts[0] == "VALUE")
            {
                key = parts[1];

                byte[] rawFlags = new byte[2];
                var totalDataLength = Convert.ToUInt32(parts[3], CultureInfo.InvariantCulture);
                byte[] bytes = new byte[totalDataLength - 2];

                if (parts.Length > 4)
                {
                    unique = Convert.ToUInt64(parts[4]);
                }
                else
                {
                    unique = 0;
                }
                socket.Read(rawFlags);
                socket.Read(bytes);
                socket.SkipUntilEndOfLine(); //Skip the trailing \r\n
                SerializedType type = (SerializedType) ConvertFromLittleEndian(rawFlags);
                try
                {
                    value = Serializer.DeSerialize(bytes, type);
                }
                catch (Exception e)
                {
                    //If deserialization fails, return null
                    value = null;
                    throw new KestrelApiException("Error deserializing key " + key + " of type " + type, e);
                }
                return true;
            }
            else
            {
                key = null;
                value = null;
                unique = 0;
                return false;
            }
        }
        #endregion

        #region Delete
        /// <summary>
        /// This method corresponds to the "delete" command in the memcache protocol.
        /// It will immediately delete the given key and corresponding value.
        /// Use the overloads to specify an amount of time the item should be in the delete queue on the server,
        /// or to specify a custom hash to override server selection.
        /// </summary>
        public bool Delete(string key) { 
            checkKey(key);

            string commandline = "DELETE " + key + "\r\n";
            Socket.WriteAndFlush(commandline);
            return Socket.ReadResponse().StartsWith("DELETED");
        }
        #endregion

        #region Flush All
        /// <summary>
        /// This method corresponds to the "flush_all" command in the memcached protocol.
        /// When this method is called, it will send the flush command to all servers, thereby deleting
        /// all items on all servers.
        /// </summary>
        public bool FlushAll()
        {
            Socket.WriteAndFlush("FLUSH_ALL\r\n");
            var response = Socket.ReadResponse();
            return response.StartsWith("Flushed all queues.");
        }


        public bool Flush(string queue)
        {
            Socket.WriteAndFlush("FLUSH " + queue + "\r\n");
            return Socket.ReadResponse().StartsWith("OK");            
        }

        #endregion

        #region Stats

        public Stats GetStats()
        {
            var retval = new Stats();
            
            Socket.WriteAndFlush("STATS\r\n");
            while (true)
            {
                var line = Socket.ReadResponse();
                if (line == "END")
                    break;

                retval.RegisterLine(line);
            }

            return retval;
        }

        #endregion
    }
}