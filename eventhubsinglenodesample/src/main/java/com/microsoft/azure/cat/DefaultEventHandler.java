package com.microsoft.azure.cat;

import com.microsoft.azure.eventhubs.EventData;

public class DefaultEventHandler implements IEventHandler {

    @Override
    public Boolean canProcess(EventData event) {
        return true;
    }

    @Override
    public String process(EventData event) {
        return "Unrecognized type";
    }
}
