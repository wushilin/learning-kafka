package io.confluent.learning.solutions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Lab4A {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("client.properties.secret"));
        props.put("group.id", "my-group");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String subscribeTopic = "test01";
        consumer.subscribe(Arrays.asList(subscribeTopic));
        int maxMessages = 1000;
        int consumedMessages = 0;
        long startTime = System.nanoTime();
        while(consumedMessages < maxMessages) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for(ConsumerRecord<String, String> next:records) {
                String key = next.key();
                String value = next.value();
                int partition = next.partition();
                long offset = next.offset();
                String topic = next.topic();
                System.out.printf("Consumed key = %s -> value = %s from %s:%d:%d\n", key, value, topic, partition, offset);
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
