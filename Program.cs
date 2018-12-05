using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace event_hub_peek
{
    class Program
    {
        static int Main(string[] args)
        {
            Console.CursorVisible = false;

            // might want to run something async at some point, stick it on the thread pool
            var source = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true;
                source.Cancel();
            };

            try
            {
                return MainAsync(args, source.Token).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return 1223;
        }

        private static async Task<int> MainAsync(string[] args, CancellationToken token)
        {
            EventHubClient client = EventHubClient.CreateFromConnectionString(args[0]);
            PartitionReceiver r = client.CreateReceiver(args[1], args[2], EventPosition.FromStart(), null);
            
            while (!token.IsCancellationRequested)
            {
                IEnumerable<EventData> received = await r.ReceiveAsync(1);
                IEnumerator<EventData> enumerator = received.GetEnumerator();
                if (enumerator.MoveNext())
                {
                    EventData data = enumerator.Current;
                    Console.WriteLine(System.Text.Encoding.UTF8.GetString(data.Body.Array));
                }
            }

            return 0;
        }
    }
}
