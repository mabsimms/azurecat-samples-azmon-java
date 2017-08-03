package com.microsoft.azure.cat;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class LocalEventProcessor
{
    final Logger logger = LoggerFactory.getLogger( LocalEventProcessor.class );
    private final String checkpointDirectory;
    private final String eventHubConnectionString;
    private final String eventHubName;
    private ExecutorService executor;
    private Gson gson;
    private String partitionIds[] = null;
    private PartitionProcessor[] processors = null;
    private String consumerGroupName;

    private LocalFileCheckpointManager checkpointManager;
    private Function<Void, IEventProcessor> userProcessorFactory;

    public LocalEventProcessor(
            String eventHubName,
            String eventHubConnectionString,
            String checkpointDirectory,
            String consumerGroupName,
            Function<Void, IEventProcessor> userProcessorFactory
    ) throws IOException, IllegalArgumentException
    {
        this.userProcessorFactory = userProcessorFactory;
        if (Strings.isNullOrEmpty(eventHubName))
            throw new IllegalArgumentException("eventHubName must not be null or empty");
        if (Strings.isNullOrEmpty(eventHubConnectionString))
            throw new IllegalArgumentException("eventConnectionString must not be null or empty");
        if (Strings.isNullOrEmpty(checkpointDirectory))
            throw new IllegalArgumentException("checkpointDirectory must not be null or empty");
        if (Strings.isNullOrEmpty(consumerGroupName))
            consumerGroupName = "$Default";

        this.userProcessorFactory = userProcessorFactory;
        this.eventHubName = eventHubName;
        this.eventHubConnectionString = eventHubConnectionString;
        this.checkpointDirectory = checkpointDirectory;
        this.consumerGroupName = consumerGroupName;
        this.executor = Executors.newCachedThreadPool();
        this.gson = new Gson();
    }

    public void Start() throws InterruptedException, ExecutionException, ServiceBusException, IOException {
        try
        {
            // Create a local checkpoint manager for saving progress to local directories
            this.checkpointManager = new LocalFileCheckpointManager(checkpointDirectory, eventHubName, true);
            checkpointManager.initialize();

            // Create a event hub client and get the event hub context and information (including partition count)
            EventHubClient eventHubClient = EventHubClient.createFromConnectionString(eventHubConnectionString).get();
            EventHubRuntimeInformation ehInfo = eventHubClient.getRuntimeInformation().get();
            if (ehInfo != null)
            {
                this.partitionIds = ehInfo.getPartitionIds();
                logger.info("EventHub {} has {} partitions", eventHubName, partitionIds.length);
            }

            // Generate a partition processor for each partition
            processors = new PartitionProcessor[partitionIds.length];
            for (int i = 0; i < partitionIds.length; i++)
            {
                String partitionId = partitionIds[i];
                IEventProcessor userProcessor = userProcessorFactory.apply(null);
                PartitionProcessor processor = new PartitionProcessor(
                        checkpointManager,
                        "TODO",
                        eventHubName,
                        eventHubConnectionString,
                        partitionId,
                        consumerGroupName,
                        userProcessor);
                processors[i] = processor;
            }

            // Initialize the processors
            for (PartitionProcessor p : processors)
            {
                logger.info("Starting processor on event hub {} for partition {}",
                    eventHubName, p.getPartitionId());
                p.Start();
            }
        }
        catch (Exception e)
        {
            logger.error("Could not instantiate local event processor", e);
            throw e;
        }
    }

    public void Stop()
    {
        for (PartitionProcessor p : processors)
        {
            logger.info("Stopping processor on event hub {} for partition {}",
                    eventHubName, p.getPartitionId());
            p.Stop();
        }

        // TODO - fix the race condition here where not all of the checkpoints have finished

        this.checkpointManager.shutdown();

        this.executor.shutdown();
        try {
            this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {

        }
    }

}
