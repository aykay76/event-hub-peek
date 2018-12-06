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
            if (args.Length < 4)
            {
                Console.WriteLine("Usage: event-hub-peek namespace-name topic-name consumer-group partition");
                Console.WriteLine("Ensure the SAS key has the correct permissions and is in the EVENT_HUB_SAS_KEY environment variable");
                return -1;
            }

            string namespaceName = args[0];
            string topicName = args[1];
            string consumerGroup = args[2];
            string partition = args[3];
            string sharedAccessKey = Environment.GetEnvironmentVariable("EVENT_HUB_SAS_KEY");
            string sharedAccessName = Environment.GetEnvironmentVariable("EVENT_HUB_SAS_NAME");

            if (sharedAccessKey == null)
            {
                Console.WriteLine("Key needs to be in EVENT_HUB_SAS_KEY environment variable");
                return -1;
            }

            Console.WriteLine($"Connecting...");

            TokenProvider tokenProvider = SharedAccessSignatureTokenProvider.CreateSharedAccessSignatureTokenProvider(sharedAccessName, sharedAccessKey);
            EventHubClient client = EventHubClient.Create(new Uri($"sb://{namespaceName}.servicebus.windows.net/"), topicName, tokenProvider);

            Console.WriteLine($"Creating receiver for consumer group {consumerGroup}, partition {partition}");

            PartitionReceiver r = client.CreateReceiver(consumerGroup, partition, EventPosition.FromStart(), null);
            
            while (!token.IsCancellationRequested)
            {
                IEnumerable<EventData> received = await r.ReceiveAsync(1);
                if (received == null)
                {
                    Console.WriteLine("Timed out, try again later.");
                    return -2;
                }
                else
                {
                	IEnumerator<EventData> enumerator = received.GetEnumerator();
                	if (enumerator.MoveNext())
                	{
                        EventData data = enumerator.Current;
                        Console.WriteLine(System.Text.Encoding.UTF8.GetString(data.Body.Array));
                	}
                }
            }

            return 0;
        }
    }
}
