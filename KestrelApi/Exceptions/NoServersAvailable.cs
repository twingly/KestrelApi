using System;

namespace Primelabs.Twingly.KestrelApi.Exceptions
{
    public class NoServersAvailable : KestrelApiException
    {
        public NoServersAvailable(string message) : base(message)
        {
        }

        public NoServersAvailable(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}