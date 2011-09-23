KestrelApi is a C# API for Kestrel (see https://github.com/robey/kestrel).
Based upon BeIT Memcached Client (see http://code.google.com/p/beitmemcached/).

License
=======
Copyright 2009 Twingly AB. MogileFsApi is based upon BeIT Memcached Client and
therefor inherits its license which is MIT. 
See the included LICENSE.txt file for specifics.

Examples
========

// Initialize a simple client for the servers kestrel1 and kestrel2
var manager = new KestrelClusterManager(new string[] { "kestrel1:22133", "kestrel2:22133" });

// Send data to the queue samplequeue
manager.Send("samplequeue", "teststring");

// Receive data from a queue as a string.
// The reads are transactional, and the default is to ack the message.
// If there is an exception or msg.Close is false, the message will
// not be acked, and will be returned to the queue.
// This method read messages from the serverlist in a random order. 
// The timeout parameter is spread out over the serverlist, meaning that 
// each read from a server will get 1/2 second timeout in this example.
// The method also does some batching, in order to not switch server 
// between each individual read. 
foreach (var msg in manager.Open<string>("samplequeue", TimeSpan.FromSeconds(1)))
{
	Console.Writeline("Received string: " + msg.Value);
}
