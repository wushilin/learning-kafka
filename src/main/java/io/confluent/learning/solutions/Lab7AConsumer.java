package io.confluent.learning.solutions;

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
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        KafkaConsumer<String, Animal> consumer = new KafkaConsumer<>(props);
        String subscribeTopic = "test02";
        consumer.subscribe(Arrays.asList(subscribeTopic));
        int maxMessages = 1000;
        int consumedMessages = 0;
        long startTime = System.nanoTime();
        while(consumedMessages < maxMessages) {
            ConsumerRecords<String, Animal> records = consumer.poll(Duration.ofMillis(500));
            for(ConsumerRecord<String, Animal> next:records) {
                String key = next.key();
                Animal value = next.value();
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
