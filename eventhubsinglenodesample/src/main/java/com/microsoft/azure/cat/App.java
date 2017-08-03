package com.microsoft.azure.cat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Paths;
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

            // Get Event Hub name and connection string from environment
            String eventHubConnectionString = "Endpoint=sb://alsonotrealz.servicebus.windows.net/;SharedAccessKeyName=AzmonReader;SharedAccessKey=notrealz;EntityPath=insights-operational-logs";
            String eventHubName = "insights-operational-logs";
            logger.info("Creating processor host");

            CreateProcessorHost(hostName, eventHubName, eventHubConnectionString, "");

            Scanner scanner = new Scanner(System.in);
            String line = scanner.nextLine();
        }
        catch (Exception e)
        {
            System.out.println(e.toString());
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

    public static void CreateProcessorHost(String hostName, String eventHubName,
                                           String eventHubConnectionString, String consumerGroup) {
        try {
            // Use the local directory as the checkpoint directory (TODO - pull from config)
            String checkpointDirectory = Paths.get(".").toAbsolutePath().normalize().toString();

            // Use the content routing processor for handling events from Event Hub
            Function<Void, IEventProcessor> factoryFunction = (Void) -> new ContentRoutingProcessor();

            // Use the local file system for checkpointing progress (only suitable for single-node durable systems).
            // For multi-node systems use the EventProcessorHost
            LocalEventProcessor localProcessor = new LocalEventProcessor(
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

    public static void ConfigureNativeLogging()
    {
        //java.util.logging.LogManager logManager = java.util.logging.LogManager.getLogManager();

        // Programmatic configuration
        //System.setProperty("java.util.logging.SimpleFormatter.format",
        //        "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] (%2$s) %5$s %6$s%n");

        //java.util.logging.ConsoleHandler consoleHandler = new java.util.logging.ConsoleHandler();
        //consoleHandler.setLevel(java.util.logging.Level.FINEST);
        //consoleHandler.setFormatter(new java.util.logging.SimpleFormatter());

        java.util.logging.Logger rootLogger = java.util.logging.LogManager.getLogManager().getLogger("");
        rootLogger.info("Testing native java.util logging");

        //rootLogger.addHandler(consoleHandler);
    }
}
