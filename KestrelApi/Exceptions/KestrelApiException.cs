using System;

namespace Primelabs.Twingly.KestrelApi.Exceptions
{
    public class KestrelApiException : ApplicationException
    {
        public KestrelApiException(string message) : base(message) {}
        public KestrelApiException(string message, Exception innerException) : base(message, innerException) {}
    }
}