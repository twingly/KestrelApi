using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using KestrelApiExample.Stats;
using Primelabs.Twingly.KestrelApi;

namespace KestrelApiExample
{
    class Program : IDisposable
    {
        static void Main(string[] args)
        {
            //var test = new Stats.StatsExample();
            var test = new LargeMessages.LargeMessagesExample();

            test.Run();
        }

        static void Main3(string[] args)
        {
            using (var test = new SlowConsumer.KestrelSlowConsumerTest())
            {
                test.Run();
                Console.WriteLine("Finished");
                Console.ReadLine();
            }
        }

        static void Main2(string[] args)
        {
            // ExerciseShuffle();
            // return;

            using (var clusterTest = new KestrelClusterTest())
            // using (var program = new Program())
            {
                try
                {
                    Console.WriteLine("*************\r\nStarting Kestrel test\r\n****************");
                    // program.ExerciseKestrel();
                    clusterTest.Run();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Got unexpected exception " + ex.ToString());
                }
                finally
                {
                    Console.WriteLine("*************\r\nEnd Kestrel test\r\n****************");
                }
            }
            Console.ReadLine();
        }

        const int socketTimeoutMilliseconds = 2000;
        KestrelProtocol protocol;
        KestrelClient client;
        const string queueResult = "channels.results";

        public static void ExerciseShuffle()
        {
            var firstInSequences = new Dictionary<int, int>();
            for (int i = 0; i < 1000000; i++)
            {
                // make a new list of 2 servers
                var l = new List<int>(new int[] {0, 1, 2, 3, 4});

                Utils.Shuffle(l);

                var firstInSequence = l[0];
                if (!firstInSequences.ContainsKey(firstInSequence))
                    firstInSequences[firstInSequence] = 0;
                firstInSequences[firstInSequence] += 1;
            }

            foreach (var key in firstInSequences.Keys.OrderBy(x => x))
                Console.WriteLine(key + " " + firstInSequences[key]);
            Console.ReadLine();
        }

        public void ExerciseKestrel()
        {
            SetupClient();

            if (!client.FlushAll())
                throw new ApplicationException("Error occured during flush all");


            // TestGetOnEmptyQueue();
            //TestLoosingConnectionDoesNotLooseMessage();

            // send 10 messages, receive 10 msgs
            // SendXWaitAndReceive(10, "no wait ", 0);
            // send 10 msgs, sleep > timeout, receive 10 msgs
            //SendXWaitAndReceive(10, "half wait ", socketTimeoutMilliseconds / 2);
            //SendXWaitAndReceive(10, "double wait ", socketTimeoutMilliseconds * 2);
            //SendXWaitAndReceive(10, "4 * wait ", socketTimeoutMilliseconds * 4);
            // SendXWaitAndReceive(10, "8 * wait ", socketTimeoutMilliseconds * 8);

            
            for (int i = 0; i < 3; i++)
            {
                Console.WriteLine("Starting run " + i + " please initialize kestrel and press enter");
                Console.ReadLine();
                SetupClient();
                //LoadTest(queueResult, 0); // normal journaled queue
                // LoadTest(queueResult, 1); // normal journaled queue
                LoadTest(queueResult, 2); // normal journaled queue
            }
            
            // LoadTest("transient_events", 0); // in memory only queue
            //LoadTest("transient_events", 1); // in memory only queue
            //LoadTest("transient_events", 2); // in memory only queue



            // What happens if we enqueue a couple of items, open a transaction, new client, peek?, get?, open?, close first connection, reopen
            // TestOpenCloseAbort();
            // TestCloseIfNoOpenItem();

            // TestOpenOneQueueCloseAnother();
            // TestOpenOneTwoQueues();
        }

        private void TestGetOnEmptyQueue()
        {
            if (!client.FlushAll())
                throw new ApplicationException("Error occured during flush all");

            // try get on empty queue
            Stopwatch sw = Stopwatch.StartNew();
            string message = "";
            if (client.Get(queueResult, out message, TimeSpan.FromSeconds(1)))
            {
                throw new Exception("Unexpted message " + message);
            }
            sw.Stop();
            Console.WriteLine("took " + sw.ElapsedMilliseconds);
        }

        private void TestLoosingConnectionDoesNotLooseMessage()
        {
            SetupClient();

            try
            {
                string value = Guid.NewGuid().ToString();
                client.Send(queueResult, value);

                string tmp;
                if (!client.Open(queueResult, out tmp, null, true))
                    throw new ApplicationException("Expected open to return value");

                if (tmp != value)
                    throw new ApplicationException("Expected value not returned");

                client.Disconnect();

                SetupClient();

                tmp = null;
                if (!client.Open(queueResult, out tmp, null, true))
                    throw new ApplicationException("Expected open to return value");

                if (tmp != value)
                    throw new ApplicationException("Expected value not returned");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Client.close() with no open item yields exception " + ex.ToString());
            }
        }

        private void TestCloseIfNoOpenItem()
        {
            SetupClient();

            try
            {
                client.Close(queueResult);
                Console.WriteLine("Seems strange, client.Close() with no open items yields no exception. ");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Client.close() with no open item yields exception " + ex.ToString());
            }
        }

        private void TestOpenOneQueueCloseAnother()
        {
            client.Send("q1", "q1 val");
            client.Send("q2", "q2 val");

            string tmp;
            if (!client.Open("q1", out tmp, null, false) || tmp != "q1 val")
                throw new ApplicationException("wrong reply expected");

            try
            {
                client.Close("q2");
                Console.WriteLine("Got no exception while closing another queue!");
            } catch (Exception ex)
            {
                Console.WriteLine("got exception while calling close on possibly tainted client");
            }

            try
            {
                client.Peek("q2", out tmp);
            } catch (Exception ex)
            {
                Console.WriteLine("Got exception while calling peek on possibly tainted client");
            }

            SetupClient();

            try
            {
                client.Peek("q2", out tmp);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Got exception while calling peek on possibly tainted client");
            }
        }

        private void TestOpenOneTwoQueues()
        {
            client.Send("q1", "q1 val");
            client.Send("q2", "q2 val");

            string tmp;
            if (!client.Open("q1", out tmp, null, false) || tmp != "q1 val")
                throw new ApplicationException("wrong reply expected");

            // this one will throw an exception, as we already have an open tnx
            bool gotException;
            try
            {
                client.Open("q2", out tmp, null, false);
                gotException = false;
            } catch (Exception ex)
            {
                gotException = true;
            }
            if (!gotException)
                throw new ApplicationException("Expected exception not gotten.");

            // this results in exception, try something else.
            //var retval = client.Close("q1");
            //Console.WriteLine("Got " + retval);
            try
            {
                client.Peek("q1", out tmp);
            } catch (Exception ex)
            {
                Console.WriteLine("Got exception on peek() on possibly tainted client");
            }
        }

        private void TestOpenCloseAbort()
        {
            if (!client.FlushAll())
                throw new ApplicationException("Error occured during flush all");

            client.Send(queueResult, "1");
            client.Send(queueResult, "2");
            client.Send(queueResult, "3");

            string firstMsg;
            if (!client.Open<string>(queueResult, out firstMsg, null, false) || firstMsg != "1")
                throw new ApplicationException("Expected msg 1");

            var client2 = CreateClient();

            string secondMsg;
            client2.Peek<string>(queueResult, out secondMsg);
            Console.WriteLine("second client peek got " + secondMsg);

            if (!client2.Open<string>(queueResult, out secondMsg, null, false) || secondMsg != "2")
                throw new ApplicationException("Expected msg 2");

            string tmp;
            client.Peek(queueResult, out tmp);
            Console.WriteLine("peeking while having an open item yields: " + tmp);

            // accept the second message
            client2.Close(queueResult);

            // abort the first the message
            client.Abort(queueResult);

            client.Peek(queueResult, out secondMsg);
            if (secondMsg != "1")
                throw new ApplicationException(string.Format("Expected msg {0} got {1}", "2", secondMsg ?? "-null-"));

        }

        private void LoadTest(string queueName, int type)
        {
            /*
            protocol.CompressionThreshold = 1024 * 128;
            SingleThreadLoadTest(queueName, 100, MultiplyString("a", 1024*1024), type);
            protocol.CompressionThreshold = 1024*1024*1024;
            SingleThreadLoadTest(queueName, 100, MultiplyString("a", 1024 * 1024), type);
            */

            // 1024 bytes / msg , 100 MB messages
            protocol.CompressionThreshold = 1024*1024*1024;
            // [10,19[ gives 1024 b to 256 kb
            // [7,10[
            for (int i = 10; i < 19; i++)
            {
                int msgSize = ((int)Math.Pow(2, i));
                const int targetByteSize = 100*1024*1024;
                int msgNumber = (targetByteSize)/Math.Max(1024, msgSize);
                SingleThreadLoadTest(queueName, msgNumber, MultiplyString("a", msgSize), type);
            }
        }

        private static string MultiplyString(string val, int times)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < times; i++)
                sb.Append(val);
            return sb.ToString();
        }

        private KestrelClient CreateClient()
        {
            WrappedSocket socket = GetSocket();
            
            var protocol = new KestrelProtocol(socket);

            var client = new KestrelClient(protocol);

            client.Connect();

            return client;
        }

        private WrappedSocket GetSocket()
        {
            //var ep = new IPEndPoint(Dns.GetHostEntry("smeagol").AddressList[0], 22133);
            var ep = new IPEndPoint(new IPAddress(new byte[4] { 192, 168, 1, 22 }), 22133);
            return new WrappedSocket(ep, socketTimeoutMilliseconds);
        }

        private void SetupClient()
        {
            if (client != null)
                client.Disconnect();

            var socket = GetSocket();
            socket.Connect();

            protocol = new KestrelProtocol(socket);

            client = new KestrelClient(protocol);


        }

        public void SendXWaitAndReceive(int nr, string prefix, int msToSleep)
        {
            for (int i = 0; i < nr; i++)
            {
                var msg = prefix + i.ToString();
                client.Send(queueResult, msg);
                Console.WriteLine(string.Format("Run {0} Put: {1}", i, msg));
            }

            System.Threading.Thread.Sleep(msToSleep);

            string tmp;
            for (int i = 0; i < nr; i++)
            {
                if (!client.Get<string>(queueResult, out tmp, null))
                    throw new ApplicationException("Expected a string in run " + i.ToString());

                Console.WriteLine(string.Format("Run {0} Got: {1}", i, tmp));
            }            
        }

        public void SingleThreadLoadTest(string queueName, int nrOfMessages, string data, int type)
        {
            var dict = new Dictionary<int, int>();
            Stopwatch sw = Stopwatch.StartNew();
            string tmp;
            int divisor = 1;
            if (nrOfMessages >= 1000)
                divisor *= 10;
            if (nrOfMessages >= 10000)
                divisor *= 10;
            if (nrOfMessages >= 100000)
                divisor *= 10;

            tmp = "xx";

            for (int i = 0; i < nrOfMessages; i++)
            {
                client.Send(queueName, data);

                if (type == 0)
                {
                    // use normal non-ack get
                    if (!client.Get<string>(queueName, out tmp, null))
                        throw new ApplicationException("Expected a string in run " + i.ToString());
                } else if (type == 1)
                {
                    // use open + separate close.
                    if (!client.Open<string>(queueName, out tmp, null, false))
                        throw new ApplicationException("Expected a string in run " + i.ToString());
                } else if (type == 2)
                {
                    if (!client.Open<string>(queueName, out tmp, null, true))
                        throw new ApplicationException("Expected a string in run " + i.ToString());                    
                }

                if (tmp != data)
                    throw new ApplicationException(
                        string.Format("Got string not equal to enqueued string:  \r\n got {0} \r\n expected {1}",
                                      tmp == null ? "-null-" : tmp.Length.ToString(),
                                      tmp == null ? "-null-" : tmp.Length.ToString()
                            ));

                if (type == 1)
                {
                    client.Close(queueName);
                }

                int secondsSinceStart = (int)sw.Elapsed.TotalSeconds;
                if (!dict.ContainsKey(secondsSinceStart))
                    dict[secondsSinceStart] = 1;
                else
                    dict[secondsSinceStart] += 1;

                // if (i % divisor == 0) Console.Write(".");
            }

            if (type == 2)
            {
                client.Close(queueName); // close last remaining tnx.
            }

            sw.Stop();

            var bytes = Encoding.UTF8.GetBytes((string)data);
            Console.WriteLine("{0};{1};{2};{3};{4};{5};",
                              type, nrOfMessages, sw.ElapsedMilliseconds, bytes.Length,
                              nrOfMessages/sw.Elapsed.TotalSeconds,
                              (nrOfMessages*bytes.Length)/sw.Elapsed.TotalSeconds
                );
            /*
            Console.WriteLine("Load test type {5} done: {0} msgs of size {3}b in {1} ms. {2} msg/second {4} bytes / second",
                              nrOfMessages, Sw.ElapsedMilliseconds, nrOfMessages/Sw.Elapsed.TotalSeconds,
                              bytes.Length, (nrOfMessages * bytes.Length) / Sw.Elapsed.TotalSeconds, type
                              );
             * */
            /*
            for (int i = 0; i < (int)Sw.Elapsed.TotalSeconds; i++)
            {
                int num = 0;
                if (dict.ContainsKey(i))
                    num = dict[i];
                Console.Write(string.Format("{0}={1},", i, num));
            }
            Console.WriteLine();
            */
        }

        public void Dispose()
        {
            if (client != null)
                client.Disconnect();
        }
    }
}
