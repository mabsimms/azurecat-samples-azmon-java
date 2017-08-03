package com.microsoft.azure.cat;

import com.microsoft.azure.eventhubs.EventData;

// This is an approximation of the IEventProcessor and PartitionContext from the
// event processor host that isn't hard coded around the actual EventProcessorHost class
public interface IEventProcessor
{
    /**
     * Called by the processor host when a batch of events has arrived.
     *
     * This is where the real work of the event processor is done. It is normally called when one
     * or more events have arrived. If the EventProcessorHost instance was set up with an EventProcessorOptions
     * on which setInvokeProcessorAfterReceiveTimeout(true) has been called, then if a receive times out,
     * it will be called with an empty iterable. By default this option is false and receive timeouts do not
     * cause a call to this method.
     *
     * @param context	Information about the partition.
     * @param messages	The events to be processed. May be empty.
     * @throws Exception
     */
    public void onEvents(PartitionContext context, EventData[] messages) throws Exception;

    /**
     * Called when the underlying client experiences an error while receiving. EventProcessorHost will take
     * care of recovering from the error and continuing to pump messages, so no action is required from
     * your code. This method is provided for informational purposes.
     *
     * @param context  Information about the partition.
     * @param error    The error that occured.
     */
    public void onError(PartitionContext context, Throwable error);
}
