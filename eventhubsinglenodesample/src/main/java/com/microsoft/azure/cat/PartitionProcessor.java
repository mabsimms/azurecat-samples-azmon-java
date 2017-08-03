package com.microsoft.azure.cat;

import com.google.common.collect.Iterables;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.ICheckpointManager;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PartitionProcessor
{
    final Logger logger = LoggerFactory.getLogger( LocalFileCheckpointManager.class );

    private final String partitionId;
    private final String eventHubPath;
    private final String consumerGroupName;
    private final String eventHubConnectionString;
    private final String hostIdentifier;

    private ReceiverRuntimeInformation runtimeInformation;
    private ICheckpointManager checkpointManager;

    private EventHubClient eventHubClient;
    private Checkpoint checkpoint;
    private PartitionReceiveHandler receiverHandler;
    private PartitionContext partitionContext;
    private IEventProcessor userProcessor;

    public PartitionProcessor(ICheckpointManager checkpointManager, String hostIdentifier,
        String eventHubPath, String eventHubConnectionString,
        String partitionId, String consumerGroupName, IEventProcessor userProcessor)
    {
        this.checkpointManager = checkpointManager;
        this.eventHubPath = eventHubPath;
        this.partitionId = partitionId;
        this.consumerGroupName = consumerGroupName;
        this.eventHubConnectionString = eventHubConnectionString;
        this.hostIdentifier = hostIdentifier;
        this.userProcessor = userProcessor;
    }

    public String getPartitionId()
    {
        return partitionId;
    }

    public void Start() throws IOException, InterruptedException, ServiceBusException, ExecutionException
    {
        // Create the partition context and grab the initial offset (default with no offset is current time)
        this.partitionContext = new PartitionContext(partitionId, eventHubPath, consumerGroupName, checkpointManager);
        Object initialOffset = partitionContext.getInitialOffset();

        // Create the event hub client used to retrieve information for this partition
        this.eventHubClient = EventHubClient.createFromConnectionString(eventHubConnectionString).get();
        ReceiverOptions options = new ReceiverOptions();
        options.setReceiverRuntimeMetricEnabled(true);
        options.setIdentifier(hostIdentifier);

        // Initialize the partition reader, either from the offset or timestamp (Instant) provided
        // by the available checkpoint
        PartitionReceiver receiver;
        if (initialOffset instanceof Instant)
        {
            logger.info("Starting partition receiver on event hub {} at partition {} from timestamp {}",
                    eventHubPath, partitionId, initialOffset);
            receiver = eventHubClient.createReceiver(
                    consumerGroupName, partitionId, (Instant)initialOffset, options).get();
        }
        else
        {
            logger.info("Starting partition receiver on event hub {} at partition {} from offset {}",
                    eventHubPath, partitionId, initialOffset);
            receiver = eventHubClient.createReceiver(
                    consumerGroupName, partitionId, initialOffset.toString(), options).get();
        }

        // Configure the receiver with prefetch and maximum wait time
        // TODO - from configuration
        receiver.setPrefetchCount(128);
        receiver.setReceiveTimeout(Duration.ofSeconds(30));

        // Set up the partition receiver and callback function
        // TODO - make max event count configurable
        this.receiverHandler = new SingleHostPartitionReceiveHandler(
                this,
                partitionContext,
                userProcessor,
                128);

        receiver.setReceiveHandler(receiverHandler, true);
    }

    class SingleHostPartitionReceiveHandler extends PartitionReceiveHandler
    {

        final Logger logger = LoggerFactory.getLogger( LocalFileCheckpointManager.class );
        final PartitionProcessor processor;
        final PartitionContext partitionContext;
        final IEventProcessor userProcessor;

        protected SingleHostPartitionReceiveHandler(PartitionProcessor processor,
            PartitionContext partitionContext, IEventProcessor userProcessor, int maxEventCount)
        {
            super(maxEventCount);

            this.processor = processor;
            this.partitionContext = partitionContext;
            this.userProcessor = userProcessor;
        }

        @Override
        public void onReceive(Iterable<EventData> events)
        {
            EventData[] data = Iterables.toArray(events, EventData.class);
            if (data.length > 0) {
                this.partitionContext.setOffsetAndSequenceNumber(data[data.length - 1]);
            }

            logger.info("Received {} events", data.length);

            for (EventData d : data)
            {
                String stringContent = new String(d.getBytes(), StandardCharsets.UTF_8);
                logger.info("Received event {}", stringContent);
            }

            try {
                userProcessor.onEvents(partitionContext, data);
            }
            catch (Exception ex)
            {
                logger.warn("Error in user processor", ex);
                userProcessor.onError(partitionContext, ex);

                // TODO - do we checkpoint on error?
            }
        }

        @Override
        public void onError(Throwable throwable)
        {
            userProcessor.onError(partitionContext, throwable);
            logger.warn("Error in partition receiver: {}", throwable.toString());
        }
    }
}
