using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace event_hub_peek
{
    public class SimpleEventProcessorFactory : IEventProcessorFactory
    {
        private readonly ConcurrentDictionary<string, SimpleEventProcessor> eventProcessors = new ConcurrentDictionary<string, SimpleEventProcessor>();
        private readonly ConcurrentQueue<SimpleEventProcessor> closedProcessors = new ConcurrentQueue<SimpleEventProcessor>();
        public string HostName { get; private set; }

        public SimpleEventProcessorFactory(string hostname)
        {
            this.HostName = hostname;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            var processor = new SimpleEventProcessor();
            processor.ProcessorClosed += this.ProcessorOnProcessorClosed;
            this.eventProcessors.TryAdd(context.Lease.PartitionId, processor);
            Console.WriteLine("👉🏻  Creating Event Processor");
            return processor;
        }

        private void ProcessorOnProcessorClosed(object sender, EventArgs eventArgs)
        {
            var processor = sender as SimpleEventProcessor;
            if (processor != null)
            {
                this.eventProcessors.TryRemove(processor.Context.Lease.PartitionId, out processor);
                this.closedProcessors.Enqueue(processor);
            }
        }
    }
}