package com.microsoft.azure.cat;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class App
{
    static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args)
    {
        try {
            logger.info("Testing main");

            String hostName = GetHostName();

            // Get Event Hub name and connection string from configuration properties
            Properties eventHubProperties = LoadProperties("eventhub.properties");
            String eventHubConnectionString = eventHubProperties.getProperty("eventhub.connectionString").replace("\"", "");;
            String eventHubName = eventHubProperties.getProperty("eventhub.name").replace("\"", "");;

            String directoryName = eventHubProperties.getProperty("eventhub.checkpointDirectory").replace("\"", "");
            String checkpointDirectory = Paths.get(directoryName).toAbsolutePath().normalize().toString();

            String consumerGroup = eventHubProperties.getProperty("eventhub.consumerGroup");
            if (Strings.isNullOrEmpty(consumerGroup))
                consumerGroup = "$Default";
            else
                consumerGroup = consumerGroup.replace("\"", "");

            logger.info("Creating processor host for event hub {} consumer group {}",
                    eventHubName, consumerGroup);
            logger.info("Using checkpoint directory {}", checkpointDirectory);

            CreateProcessorHost(hostName, eventHubName, eventHubConnectionString, consumerGroup, checkpointDirectory);

            Scanner scanner = new Scanner(System.in);
            String line = scanner.nextLine();

            logger.info("Close signal received; shutting down receiver");
            CloseProcessorHost();
            logger.info("Close complete; exiting application");

            // TODO - fix the thread pool shutdown issue blocking the app from exiting cleanly
            System.exit(0);
        }
        catch (Exception e)
        {
            System.out.println(e.toString());
        }
    }

    private static void CloseProcessorHost() {
        if (localProcessor == null)
            return;

        localProcessor.Stop();
    }

    private static LocalEventProcessor localProcessor;

    public static void CreateProcessorHost(String hostName, String eventHubName,
        String eventHubConnectionString, String consumerGroup, String checkpointDirectory)
    {
        try {
            // Use the content routing processor for handling events from Event Hub
            Function<Void, IEventProcessor> factoryFunction = (Void) -> new ContentRoutingProcessor();

            // Use the local file system for checkpointing progress (only suitable for single-node durable systems).
            // For multi-node systems use the EventProcessorHost
            localProcessor = new LocalEventProcessor(
                    eventHubName, eventHubConnectionString, checkpointDirectory,
                    consumerGroup, factoryFunction);
            localProcessor.Start();
        }
        catch (Exception e)
        {
            logger.error("Failure while registering event processor host", e);
            if (e instanceof ExecutionException) {
                Throwable inner = e.getCause();
                logger.error(inner.toString());
            }
        }
    }

    public static String GetHostName()
    {
        try
        {
            String hostName = InetAddress.getLocalHost().getHostName();
            return hostName;
        }
        catch (IOException e)
        {
            return "NoHostnameAvailable";
        }
    }

    static Properties LoadProperties(String resourceName) throws IOException
    {
        logger.info("Loading resource {}", resourceName);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            if (resourceStream == null)
                throw new IOException("Could not open resource file " + resourceName);
            props.load(resourceStream);
        }
        return props;
    }
 }
