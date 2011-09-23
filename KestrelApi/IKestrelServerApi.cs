using System;
using System.Linq;
using System.Text;

namespace Primelabs.Twingly.KestrelApi
{
    public interface IKestrelServerApi 
    {
        // hur ska interfacet se ut?
        // påbörja design. flytta över själva memcache-protokoll-impl, socket-kod osv. ta bort det som är onödigt, byt namespace, men behåll copyright noticen.
        // någon form av disposable operationer för köerna? inte säkert att det är det bästa dock, snöa inte in på det..

        bool Send(string queueName, object value);
        bool Send(string queueName, object value, TimeSpan expiresIn);
        bool Send(string queueName, object value, DateTime expiresAt);
        
        bool Peek<T>(string queueName, out T obj);

        // Unknowledge read-operation
        bool Get<T>(string queueName, out T value, TimeSpan? timeout);

        // Acknowledged read-operations below
        bool Open<T>(string queueName, out T value, TimeSpan? timeout, bool close);
        void Close(string queueName);

        void Abort(string queueName);
    }

    /*
    public class TestReciver
    {
        public void ThreadProc()
        {
            // vi vill ha möjlighet att skicka till vilken server som helst i klustret.
            // för receive-operationer, så vill generellt bara ta emot från en kö. vi ha möjlighet att köra /close/open några gånger

            // while _shouldExit:
            //   ReadAndAck(
        }
    }
     * */
}
