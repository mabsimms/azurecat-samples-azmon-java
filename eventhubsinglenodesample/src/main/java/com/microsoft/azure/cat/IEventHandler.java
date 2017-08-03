package com.microsoft.azure.cat;

import com.microsoft.azure.eventhubs.EventData;

import java.util.function.Function;

public interface IEventHandler
{
    Boolean canProcess(EventData event);
    String process(EventData event) throws Exception;
}
