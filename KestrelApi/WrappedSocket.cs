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
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Primelabs.Twingly.KestrelApi.Exceptions;

namespace Primelabs.Twingly.KestrelApi
{
    /// <summary>
    /// The WrappedSocket class encapsulates a socket connection to a specified memcached server.
    /// It contains a buffered stream for communication, and methods for sending and retrieving
    /// data from the memcached server, as well as general memcached error checking.
    /// </summary>
    public class WrappedSocket : IDisposable
    {
        private readonly IPEndPoint _endPoint;
        private readonly int _sendReceiveTimeout;
        private static LogAdapter logger = LogAdapter.GetLogger(typeof(WrappedSocket));

        private Socket socket;
        private Stream stream;
        public readonly DateTime Created;

        public WrappedSocket(IPEndPoint endPoint, int sendReceiveTimeout)
        {
            _endPoint = endPoint;
            _sendReceiveTimeout = sendReceiveTimeout;
            Created = DateTime.Now;
        }

        public void Connect()
        {
            Close();

            //Set up the socket.
            socket = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, _sendReceiveTimeout);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, _sendReceiveTimeout);
            socket.ReceiveTimeout = _sendReceiveTimeout;
            socket.SendTimeout = _sendReceiveTimeout;

            //Do not use Nagle's Algorithm
            socket.NoDelay = true;
            //Establish connection
            socket.Connect(EndPoint);

            //Wraps two layers of streams around the socket for communication.
            stream = new BufferedStream(new NetworkStream(socket, false));            
        }

        /// <summary>
        /// Releases all resources used by this instance and shuts down the inner <see cref="T:Socket"/>. This instance will not be usable anymore.
        /// </summary>
        /// <remarks>Use the IDisposable.Dispose method if you want to release this instance back into the pool.</remarks>
        public void Destroy()
        {
            this.Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                GC.SuppressFinalize(this);

                if (socket != null)
                {
                    using (this.socket)
                        this.socket.Shutdown(SocketShutdown.Both);
                }

                this.stream.Dispose();

                this.stream = null;
                this.socket = null;
            }
            else
            {
                // Do nothing atm, this is run when someone uses this socket in a using-statement.
            }
        }

        /// <summary>
        /// Disposing of a WrappedSocket object in any way causes it to be returned to its SocketPool.
        /// </summary>
        void IDisposable.Dispose()
        {
            this.Dispose(false);
        }
        
        /// <summary>
        /// This method closes the underlying stream and socket.
        /// </summary>
        public void Close()
        {
            if (stream != null)
            {
                try { stream.Close(); }
                catch (Exception e) { logger.Error("Error closing stream: " + this.EndPoint, e); }
                stream = null;
            }
            if (socket != null)
            {
                try { socket.Shutdown(SocketShutdown.Both); }
                catch (Exception e) { logger.Error("Error shutting down socket: " + EndPoint, e); }
                try { socket.Close(); }
                catch (Exception e) { logger.Error("Error closing socket: " + EndPoint, e); }
                socket = null;
            }
        }

        /// <summary>
        /// Checks if the underlying socket and stream is connected and available.
        /// </summary>
        public bool IsAlive
        {
            get { return socket != null && socket.Connected && stream != null && stream.CanRead; }
        }

        public IPEndPoint EndPoint
        {
            get { return _endPoint; }
        }

        /// <summary>
        /// Writes a string to the socket encoded in UTF8 format.
        /// </summary>
        public void Write(string str)
        {
            Write(Encoding.UTF8.GetBytes(str));
        }

        public void WriteAndFlush(string str)
        {
            Write(str);
            Flush();
        }

        /// <summary>
        /// Writes an array of bytes to the socket and flushes the stream.
        /// </summary>
        public void Write(byte[] bytes)
        {
            stream.Write(bytes, 0, bytes.Length);
        }

        public void Flush()
        {
            stream.Flush();
        }

        /// <summary>
        /// Reads from the socket until the sequence '\r\n' is encountered, 
        /// and returns everything up to but not including that sequence as a UTF8-encoded string
        /// </summary>
        public string ReadLine()
        {
            MemoryStream buffer = new MemoryStream();
            int b;
            bool gotReturn = false;
            while ((b = stream.ReadByte()) != -1)
            {
                if (gotReturn)
                {
                    if (b == 10)
                    {
                        break;
                    }
                    else
                    {
                        buffer.WriteByte(13);
                        gotReturn = false;
                    }
                }
                if (b == 13)
                {
                    gotReturn = true;
                }
                else
                {
                    buffer.WriteByte((byte)b);
                }
            }
            return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
        }

        /// <summary>
        /// Reads a response line from the socket, checks for general memcached errors, and returns the line.
        /// If an error is encountered, this method will throw an exception.
        /// </summary>
        public string ReadResponse()
        {
            string response = "";
            try
            {
                response = ReadLine();
            } catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
            if (String.IsNullOrEmpty(response))
            {
                throw new KestrelApiException("Received empty response.");
            }

            if (response.StartsWith("ERROR")
                || response.StartsWith("CLIENT_ERROR")
                || response.StartsWith("SERVER_ERROR"))
            {
                throw new KestrelApiException("Server returned " + response);
            }

            return response;
        }

        /// <summary>
        /// Fills the given byte array with data from the socket.
        /// </summary>
        public void Read(byte[] bytes)
        {
            if (bytes == null)
            {
                return;
            }

            int readBytes = 0;
            while (readBytes < bytes.Length)
            {
                readBytes += stream.Read(bytes, readBytes, (bytes.Length - readBytes));
            }
        }

        /// <summary>
        /// Reads from the socket until the sequence '\r\n' is encountered.
        /// </summary>
        public void SkipUntilEndOfLine()
        {
            int b;
            bool gotReturn = false;
            while ((b = stream.ReadByte()) != -1)
            {
                if (gotReturn)
                {
                    if (b == 10)
                    {
                        break;
                    }
                    else
                    {
                        gotReturn = false;
                    }
                }
                if (b == 13)
                {
                    gotReturn = true;
                }
            }
        }

        /// <summary>
        /// Resets this WrappedSocket by making sure the incoming buffer of the socket is empty.
        /// If there was any leftover data, this method return true.
        /// </summary>
        public bool Reset()
        {
            this.stream.Flush();
            if (socket.Available > 0)
            {
                byte[] b = new byte[socket.Available];
                Read(b);
                return true;
            }
            return false;
        }
    }
}
