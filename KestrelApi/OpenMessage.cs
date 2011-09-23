namespace Primelabs.Twingly.KestrelApi
{
    public class OpenMessage<T> : IOpenMessage<T>
    {
        public T Value { get; set; }

        public bool Close { get; set; }
    }
}