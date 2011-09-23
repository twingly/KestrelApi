namespace Primelabs.Twingly.KestrelApi
{
    public interface IOpenMessage<T>
    {
        T Value { get; set;  }

        bool Close { get; set; }
    }
}