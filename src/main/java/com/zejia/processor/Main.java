package com.zejia.processor;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.models.PartitionContext;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Main {

    final static String CONNECTION_STRING = "<eventhub-connection-string>";

    final static String EVENTHUB_NAME = "<eventhub-name-2>";

    public static void main(String[] args) throws InterruptedException {
        EventProcessorClient client = new EventProcessorClientBuilder()
                .connectionString(CONNECTION_STRING, EVENTHUB_NAME)
                .processEvent(eventContext -> {
                    System.out.println("---------------------------------------------------------------------------------------");
                    System.out.println(Arrays.toString(eventContext.getEventData().getBody()));
                    System.out.println(new String(eventContext.getEventData().getBody(), StandardCharsets.UTF_8));
                    System.out.println(eventContext.getEventData().getRawAmqpMessage().getProperties().getContentEncoding());
                    System.out.println(eventContext.getEventData().getRawAmqpMessage().getProperties().getContentType());
                    System.out.println(eventContext.getEventData().getRawAmqpMessage().getMessageAnnotations());
                    System.out.println(eventContext.getEventData().getRawAmqpMessage().getDeliveryAnnotations());
                    System.out.println("---------------------------------------------------------------------------------------");
                    eventContext.updateCheckpoint();
                })
                .processError(errorContext -> {
                    final PartitionContext partition = errorContext.getPartitionContext();
                    System.out.printf("Error occurred processing partition %s, %s %n", partition.getPartitionId(),
                            errorContext.getThrowable().toString());
                })
                .processPartitionInitialization(initializationContext -> {
                    final PartitionContext partition = initializationContext.getPartitionContext();
                    System.out.printf("start partition id %s %n", partition.getPartitionId());
                })
                .checkpointStore(new SampleCheckpointStore())
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .transportType(AmqpTransportType.AMQP_WEB_SOCKETS)
                .buildEventProcessorClient();


        client.start();
        System.out.println("started!");

        TimeUnit.MINUTES.sleep(10L);
    }
}
