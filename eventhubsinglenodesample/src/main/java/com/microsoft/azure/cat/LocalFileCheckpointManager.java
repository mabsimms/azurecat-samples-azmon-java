package com.microsoft.azure.cat;

import com.google.common.base.Strings;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.ICheckpointManager;
import com.microsoft.azure.eventprocessorhost.Lease;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileCheckpointManager implements ICheckpointManager
{
    final Logger logger = LoggerFactory.getLogger( LocalFileCheckpointManager.class );

    private EventProcessorHost host;
    private Path checkpointDirectory;
    private Path eventHubDirectory;
    private ExecutorService executor;
    private String eventHubName;
    private Gson gson;

    public LocalFileCheckpointManager(String checkpointDirectory, String eventHubName)
            throws IOException
    {
        this(checkpointDirectory, eventHubName,true);
    }

    public LocalFileCheckpointManager(String checkpointDirectory, String eventHubName, Boolean autoCreatePath)
            throws IOException
    {
        if (Strings.isNullOrEmpty(checkpointDirectory))
            throw new IOException("Invalid or empty directory for checkpoint base directory");
        if (Strings.isNullOrEmpty(eventHubName))
            throw new IOException("Invalid or empty directory for eventhubname");
        this.eventHubName = eventHubName;

        Path checkpointPath = FileSystems.getDefault().getPath(checkpointDirectory);
        if (!Files.exists(checkpointPath, LinkOption.NOFOLLOW_LINKS))
        {
            if (!autoCreatePath)
            {
                throw new IOException("Path " + checkpointDirectory + " does not exist and autoCreatePath set to false");
            }
            else
            {
                // Attempt to create the directory if it doesn't exist
                logger.info("Creating checkpoint directory {}", checkpointPath);
                Files.createDirectory(checkpointPath);
            }
        }
        this.checkpointDirectory = checkpointPath;

        // Note: the current implementation of the EventProcessorHost
        // restricts access to its internal executor service, so we'll
        // need to create our own.
        this.executor = Executors.newCachedThreadPool();
        this.gson = new Gson();
    }

    public void initialize() throws IOException
    {
        eventHubDirectory = checkpointDirectory.resolve(eventHubName);
        logger.info("Initializing checkpoint manager for event hub {}", eventHubName);

        if (!Files.exists(eventHubDirectory))
        {
            // Attempt to create the directory if it doesn't exist
            Files.createDirectory(eventHubDirectory);
            logger.info("Created checkpoint directory for EventHub {}: {}", eventHubName, eventHubDirectory);
        }
    }

    public Future<Boolean> checkpointStoreExists() {
        return executor.submit(() -> {
            logger.info("Checking for checkpoint store in {}", eventHubDirectory);
            return Files.exists(eventHubDirectory);
        });
    }

    public Future<Boolean> createCheckpointStoreIfNotExists()
    {
        return executor.submit(() -> {
            try
            {
                if (!Files.exists(eventHubDirectory))
                {
                    // Attempt to create the directory if it doesn't exist
                    Files.createDirectory(eventHubDirectory);
                    logger.info("Created checkpoint directory for EventHub {}: {}", eventHubName, eventHubDirectory);
                }
                return true;
            }
            catch (IOException e)
            {
                logger.warn("Error creating checkpoint store for event hub {}" + eventHubName, e);
                return false;
            }
        });
    }

    public Future<Boolean> deleteCheckpointStore() {
        return executor.submit(() -> {
            try
            {
                if (Files.exists(eventHubDirectory))
                {
                    logger.info("Deleting checkpoint directory for EventHub {}: {}", eventHubName, eventHubDirectory);

                    Files.walk(eventHubDirectory, FileVisitOption.FOLLOW_LINKS)
                            .map(Path::toFile)
                            .forEach(File::delete);
                    Files.delete(eventHubDirectory);
                }
                return true;
            }
            catch (IOException e)
            {
                logger.warn("Error deleting checkpoint store for event hub {}" + eventHubName, e);
                return false;
            }
        });
    }

    public Future<Checkpoint> getCheckpoint(String partitionId) {
        return executor.submit(() -> GetCheckpoint(partitionId));
    }

    public Future<Checkpoint> createCheckpointIfNotExists(String partitionId)
    {
        return executor.submit(() -> {
            try
            {
                Checkpoint checkpoint = GetCheckpoint(partitionId);
                if (checkpoint == null)
                {
                    // Create an empty checkpoint
                    checkpoint = new Checkpoint("0");
                    checkpoint.setOffset("0");
                    checkpoint.setSequenceNumber(0);

                    byte[] raw = gson.toJson(checkpoint).getBytes(StandardCharsets.UTF_8);
                    Path checkpointFile = eventHubDirectory.resolve(partitionId);

                    Files.write(checkpointFile, raw);

                    logger.info("Wrote out checkpoint data for event hub {}: {}",
                            eventHubName, gson.toJson(checkpoint));
                }

                return checkpoint;
            }
            catch (Exception e)
            {
                logger.warn("Error creating checkpoint for partition " + partitionId, e);
                return null;
            }
        });
    }

    public Future<Void> updateCheckpoint(Lease lease, Checkpoint checkpoint) {
        return (Future<Void>) executor.submit(() ->
        {
            try {
                byte[] raw = gson.toJson(checkpoint).getBytes(StandardCharsets.UTF_8);
                Path checkpointFile = eventHubDirectory.resolve(checkpoint.getPartitionId());
                Files.write(checkpointFile, raw);

                logger.info("Updating checkpoint file {} for event hub {} with {}",
                        checkpointFile, eventHubName, gson.toJson(checkpoint));
            }
            catch (IOException e)
            {
                logger.warn("Error updating checkpoint for partition " + checkpoint.getPartitionId(), e);
            }
        });
    }

    public Future<Void> updateCheckpoint(Checkpoint checkpoint) {
        throw new RuntimeException("Use updateCheckpoint(checkpoint, lease) instead.");
    }

    public Future<Void> deleteCheckpoint(String partitionId) {
        return (Future<Void>) executor.submit(() -> {
            try {
                Path checkpointFile = eventHubDirectory.resolve(partitionId);
                if (Files.exists(checkpointFile)) {
                    Files.delete(checkpointFile);

                    logger.info("Deleting checkpoint file {} for event hub {}",
                            checkpointFile, eventHubName);
                }
            }
            catch (IOException e)
            {
                logger.warn("Error deleting checkpoint for partition " + partitionId, e);
            }
        });
    }

    /////////////////////////////////////////////////////////////
    // Internal helper functions
    protected Checkpoint GetCheckpoint(String partitionId)
    {
        Checkpoint checkpoint = null;

        try {
            Path checkpointFile = eventHubDirectory.resolve(partitionId);
            if (!Files.exists(checkpointFile))
                return null;

            byte[] raw = Files.readAllBytes(checkpointFile);
            String content = new String(raw, StandardCharsets.UTF_8);
            checkpoint = gson.fromJson(content, Checkpoint.class);

            logger.debug("Retrieved checkpoint file {} for event hub {} with {}",
                    checkpointFile, eventHubName, gson.toJson(checkpoint));
        }
        catch (Exception e)
        {
            logger.error("Could not retrieve checkpoint for " + partitionId, e);
        }
        return checkpoint;
    }

    Iterable<Checkpoint> GetAllCheckpoints() throws IOException
    {
        ArrayList<Checkpoint> checkpoints = new ArrayList<Checkpoint>();
        Files.walk(eventHubDirectory, FileVisitOption.FOLLOW_LINKS)
                .map(Path::toFile)
                .filter(File::isFile)
                .forEach(c -> {
                    String partitionId = c.getName();
                    Checkpoint cp = GetCheckpoint(partitionId);
                    checkpoints.add(cp);
                });
        return checkpoints;
    }
}
