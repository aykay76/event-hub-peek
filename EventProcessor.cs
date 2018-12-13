using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System.Threading;
using System.Threading.Tasks;

namespace event_hub_peek
{
    public class SimpleEventProcessor : IEventProcessor
    {
        public PartitionContext Context { get; private set; }
        public event EventHandler ProcessorClosed;
        public bool IsInitialised { get; private set; }
        public bool IsClosed { get; private set; }

        public SimpleEventProcessor()
        {
            this.IsClosed = false;
            this.IsInitialised = false;
        }

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Processor Shutting Down. Partition '{context.PartitionId}', Reason: '{reason}'.");
            this.IsClosed = true;
            this.OnProcessorClosed();
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            this.Context = context;

            Console.WriteLine($"SimpleEventProcessor initialized. Partition: '{context.PartitionId}'");
            this.IsInitialised = true;

            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.WriteLine($"Error on Partition: {context.PartitionId}, Error: {error.Message}");
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                var data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                Console.WriteLine($"Message received. Partition: '{context.PartitionId}', Data: '{data}'");
            }

            return context.CheckpointAsync();
        }

        protected virtual void OnProcessorClosed()
        {
            EventHandler handler = this.ProcessorClosed;
            if (handler != null)
            {
                handler(this, EventArgs.Empty);
            }
        }
    } 
}