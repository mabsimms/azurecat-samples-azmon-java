package com.microsoft.azure.cat.eventhandlers;

import com.microsoft.azure.cat.IEventHandler;
import com.microsoft.azure.eventhubs.EventData;

public class ActivityLogHandler implements IEventHandler
{
    @Override
    public Boolean canProcess(EventData event)
    {
        return false;
    }

    @Override
    public String process(EventData event)
    {
        return "activity log";
    }
}
