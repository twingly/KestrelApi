using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Primelabs.Twingly.KestrelApi
{
    public class Stats
    {
        public Stats()
        {
            GlobalStats = new Dictionary<string, string>();
            QueueStats = new Dictionary<string, Dictionary<string, string>>();
        }

        public Dictionary<string, string> GlobalStats { get; private set; }
        public Dictionary<string, Dictionary<string, string>> QueueStats { get; private set; }

        // STAT queue_recon_blogstreamdb_open_transactions 0
        // $statkeys = array('items', 'bytes', 'total_items', 'logsize', 'expired_items', 'mem_items', 'mem_bytes', 'age', 'discarded', 'waiters', 'open_transactions');
        // $regex = sprintf("/queue_(.*?)_(%s)/", implode('|', $statkeys));

        private static Regex _regexValidator = new Regex("^queue_(.*?)_(items|bytes|total_items|logsize|expired_items|mem_items|mem_bytes|age|discarded|waiters|open_transactions)\\s+(.*)$", RegexOptions.Compiled);

        public void RegisterLine(string line)
        {
            line = line.Trim();

            if (!line.StartsWith("STAT "))
                return;
            line = line.Substring(5);

            if (! line.StartsWith("queue_"))
            {
                // this is global stats.
                var arr = line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);
                if (arr.Length < 2)
                    return;

                GlobalStats[arr[0]] = arr[1];
                return;
            }
            
            var match = _regexValidator.Match(line);
            if (!match.Success)
                return;

            var queueName = match.Groups[1].Value;
            var statsKey = match.Groups[2].Value;
            var value = match.Groups[3].Value;

            if (!QueueStats.ContainsKey(queueName))
                QueueStats[queueName] = new Dictionary<string, string>();

            QueueStats[queueName][statsKey] = value;
        }
    }
}