package io.confluent.learning.labs;

import com.example.Animal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Lab7AConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("client.properties.secret"));
        props.put("group.id", "my-group");
        props.put("auto.offset.reset", "earliest");
        props.put("specific.avro.reader", "true");
        props.put("key.deserializer", "???"); // TODO: Set some deserializer similar to Producer?
        props.put("value.deserializer", "???"); // TODO: Set some deserializer?
        KafkaConsumer<String, Animal> consumer = new KafkaConsumer<>(props);
        String subscribeTopic = ""; // TODO: which topic to read from?
        consumer.subscribe(Arrays.asList(subscribeTopic));
        int maxMessages = 1000;
        int consumedMessages = 0;
        long startTime = System.nanoTime();
        while(consumedMessages < maxMessages) {
            ConsumerRecords<String, Animal> records = consumer.poll(Duration.ofMillis(500));
            for(ConsumerRecord<String, Animal> next:records) {
                // TODO: Print the key and values, and their metadata including: Topic, Partition and offset
                consumedMessages++;
            }
        }
        consumer.commitSync();
        long endTime = System.nanoTime();
        double elapsedSec = (endTime - startTime) / 1_000_000_000.0;
        double rate = consumedMessages / elapsedSec;

        System.out.printf("Consumed %,d messages in %.2f seconds (%.2f msg/sec)\n", consumedMessages, elapsedSec, rate);
    }
}
