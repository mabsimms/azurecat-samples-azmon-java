package com.microsoft.azure.cat;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ReceiverRuntimeInformation;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.ICheckpointManager;
import com.microsoft.azure.eventprocessorhost.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PartitionContext
{
    final Logger logger = LoggerFactory.getLogger( PartitionContext.class );

    private final String partitionId;
    private final String eventHubPath;
    private final String consumerGroupName;
    private final ICheckpointManager checkpointManager;

    private String offset = PartitionReceiver.START_OF_STREAM;
    private long sequenceNumber = 0;
    private ReceiverRuntimeInformation runtimeInformation;

    public PartitionContext(String partitionId, String eventHubPath,
        String consumerGroupName, ICheckpointManager checkpointManager)
    {
        this.partitionId = partitionId;
        this.eventHubPath = eventHubPath;
        this.consumerGroupName = consumerGroupName;
        this.checkpointManager = checkpointManager;
    }

    public String getConsumerGroupName()
    {
        return this.consumerGroupName;
    }

    public String getEventHubPath()
    {
        return this.eventHubPath;
    }

    public ReceiverRuntimeInformation getRuntimeInformation()
    {
        return this.runtimeInformation;
    }

    void setRuntimeInformation(ReceiverRuntimeInformation value)
    {
        this.runtimeInformation = value;
    }


    void setOffsetAndSequenceNumber(EventData event)
    {
        if (sequenceNumber >= this.sequenceNumber)
        {
            this.offset = event.getSystemProperties().getOffset();
            this.sequenceNumber = event.getSystemProperties().getSequenceNumber();
        }
        else
        {
            logger.warn("setOffsetAndSequenceNumber for partition {}: {}/{} would move backwards; ignoring",
                    this.partitionId, event.getSystemProperties().getOffset(), event.getSystemProperties().getSequenceNumber());
        }
    }


    // Returns a String (offset) or Instant (timestamp).
    Object getInitialOffset() throws InterruptedException, ExecutionException
    {
        Object startAt = null;

        Checkpoint startingCheckpoint = checkpointManager.getCheckpoint(this.partitionId).get();
        if (startingCheckpoint == null)
        {
            // TODO - make this configurable
            // No checkpoint was ever stored. Start from current time
            //startAt = Instant.now();
            startAt = 0;
        }
        else
        {
            // Checkpoint is valid, use it.
            this.offset = startingCheckpoint.getOffset();
            startAt = this.offset;
            this.sequenceNumber = startingCheckpoint.getSequenceNumber();

            logger.info("Retrieved starting offset for partitionId {} with {}/{}",
                    partitionId, offset, sequenceNumber);
        }

        return startAt;
    }



    /**
     * Writes the current offset and sequenceNumber to the checkpoint store via the checkpoint manager.
     * @throws IllegalArgumentException  If this.sequenceNumber is less than the last checkpointed value
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void checkpoint() throws IllegalArgumentException, InterruptedException, ExecutionException
    {
        // Capture the current offset and sequenceNumber. Synchronize to be sure we get a matched pair
        // instead of catching an update halfway through. The capturing may not be strictly necessary,
        // since checkpoint() is called from the user's event processor which also controls the retrieval
        // of events, and no other thread should be updating this PartitionContext, unless perhaps the
        // event processor is itself multithreaded... Whether it's required or not, the amount of work
        // required is trivial, so we might as well do it to be sure.
        Checkpoint capturedCheckpoint = new Checkpoint(this.partitionId, this.offset, this.sequenceNumber);
        checkpointManager.updateCheckpoint((Lease) null, capturedCheckpoint).get();
    }
}