package com.microsoft.azure.cat;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentRoutingProcessor implements IEventProcessor
{
    final Logger logger = LoggerFactory.getLogger( ContentRoutingProcessor.class );

    public ContentRoutingProcessor()
    {
        logger.info("Creating content routing processor");
    }

    @Override
    public void onEvents(PartitionContext context, EventData[] messages) throws Exception
    {
        for (EventData event : messages)
        {

        }

        logger.debug("Checkpointing progress");
        context.checkpoint();
    }

    @Override
    public void onError(PartitionContext context, Throwable error) {

    }
}
