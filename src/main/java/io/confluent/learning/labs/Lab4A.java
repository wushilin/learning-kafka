package io.confluent.learning.labs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Lab4A {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("client.properties.secret"));
        props.put("group.id", "???"); // TODO: Set a consumer group ID
        props.put("auto.offset.reset", "earliest"); // TODO: Understand why
        props.put("key.deserializer", "???"); // TODO: Set deserializer for key
        props.put("value.deserializer", "???"); // TODO: Set deserailizer for value
        KafkaConsumer<String, String> consumer = null; // TODO: Initialize the consumer similar to the producer
        String subscribeTopic = "";

        // TODO: Subscribe the consumer to the topics using consumer.subscribe API
        int maxMessages = 1000; // Max messages to consume(typically not required)
        int consumedMessages = 0;
        long startTime = System.nanoTime();
        while(consumedMessages < maxMessages) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for(ConsumerRecord<String, String> next:records) {
                // TODO: Get the message's key and value, and try to identify where the message from:
                //    TOPIC, PARTITION, OFFSET
                consumedMessages++;
            }
        }
        // TODO: Maybe commit your offset here so you can resume from this point onwards
        long endTime = System.nanoTime();
        double elapsedSec = (endTime - startTime) / 1_000_000_000.0;
        double rate = consumedMessages / elapsedSec;

        System.out.printf("Consumed %,d messages in %.2f seconds (%.2f msg/sec)\n", consumedMessages, elapsedSec, rate);
    }
}
