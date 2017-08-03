package com.microsoft.azure.cat;

import com.google.common.base.Strings;
import com.microsoft.azure.cat.eventhandlers.ActivityLogHandler;
import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentRoutingProcessor implements IEventProcessor
{
    final Logger logger = LoggerFactory.getLogger( ContentRoutingProcessor.class );

    public ContentRoutingProcessor()
    {
        logger.info("Creating content routing processor");
        defaultHandler = new DefaultEventHandler();

        handlers = new IEventHandler[] {
            new ActivityLogHandler()
        };
    }

    private IEventHandler[] handlers;
    private IEventHandler defaultHandler;

    @Override
    public void onEvents(PartitionContext context, EventData[] messages) throws Exception
    {
        // TODO - implement parallel processing here
        for (EventData event : messages)
        {
            processEvent(event);
        }

        logger.debug("Checkpointing progress for event hub {} on partition {} to offset {}",
                context.getEventHubPath(), context.getPartitionId(),
                context.getCurrentOffset());
        context.checkpoint();
    }

    void processEvent(EventData event)
    {
        try
        {
            Boolean processed = false;

            for (IEventHandler handler : handlers)
            {
                if (handler.canProcess(event))
                {
                    String processedValue = handler.process(event);
                    if (!Strings.isNullOrEmpty(processedValue))
                    {
                        processed = true;
                        // TODO publish somewhere

                        // TODO - is cardinality on processors 1:1 or 1:N?
                        // If 1:1, then return  below is fine
                        return;
                    }
                }
            }

            if (!processed)
            {
                defaultHandler.process(event);
            }
        }
        catch (Exception e)
        {
            logger.warn("Could not process message", e);

            // TODO - log this message to the file system for later reprocessing and analysis
        }
    }

    @Override
    public void onError(PartitionContext context, Throwable error) {

    }
}
