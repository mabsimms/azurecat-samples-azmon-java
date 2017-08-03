# Azure Monitor Single-Node Reader Sample (Java)

This is a simple reference sample to demonstrate how to use the Event Hub SDK for Java to create 
a single-node (non-distributed) reader for all available partitions of a configured event hub.  

This sample uses the local file system for handling checkpoints (progress within the stream), and
is not suitable for distributed deployment.

## Running

The reference has a simple console application defined in `App`.  Run this application (no special 
configuration set required) directly, or in the GUI or choice (IntelliJ Community edition was used
for dev).  

## Configuring

Provide an eventhub.properties in the `resources/` directory with the event hub name, shared access
key signature (`Listen` permissions) and [optionally] consumer group name.  Use the sample 
`eventhub.properties.sample` as a reference.

```
#########################################################################
# Event Hub properties for reading Azure Monitor and related information

# The name (path) of the event hub
eventhub.name="insights-operational-logs"

# The shared access policy based connection string (with Listen) permissions for this event hub
eventhub.connectionString="Endpoint=sb://yournamespacehere.servicebus.windows.net/;SharedAccessKeyName=AzmonReader;SharedAccessKey=[shared access key here];EntityPath=insights-operational-logs"

# Checkpoint directory.  Leave blank to use working directory of the app
eventhub.checkpointDirectory="checkpoints/"

# Consumer group name; leave blank to use "$Default"
eventhub.consumerGroup="$Default"
```

## Adding handlers

The default application is set up to use a chain of content-based routing handlers with 1:1 cardinality 
(i.e. each message can only be processed by a single handler).  This is intended to allow identification 
of interesting message types and association with a mapping + publisher function.

To add a new handler, create a new class that implements the `IEventHandler` interface:

```java
public interface IEventHandler
{
    Boolean canProcess(EventData event);
    String process(EventData event) throws Exception;
}
```

Such as:

```java
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
```

Then add it to the list in the constructor of `ContentRoutingProcessor`:

```java
handlers = new IEventHandler[] {
    new ActivityLogHandler()
};
```

## Open issues

See the issues tracker for the most up to date 

- Max event count, receiver prefetch count and receive timeout are hard coded, not pulled from configuration
- Starting checkpoint is hard-coded to offset 0; should be configurable to offet 0 or current time
- Shutdown is not yet cleanly implements in terms of waiting for background tasks and threads
- Host identifier hard coded as "TODO"
- Records which have failed processing are not written to a durable archive for post-processing
- Event data parsing currently serial (likely not a problem for simple message conversion and batch writing)
- The content routing processor does not implement an output function to publish data "somewhere"



