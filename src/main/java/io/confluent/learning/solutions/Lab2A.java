package io.confluent.learning.solutions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.util.Properties;

public class Lab2A {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("client.properties.secret"));

        int numMessages = 1000; // send 1000 messages
        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "test01";
        long startTime = System.nanoTime();
        for (int i = 0; i < numMessages; i++) {
            String key = String.valueOf(i);
            String value = String.format("%s-%d", "value", System.currentTimeMillis());
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
        }
        producer.flush();
        producer.close();

        long endTime = System.nanoTime();
        double elapsedSec = (endTime - startTime) / 1_000_000_000.0;
        double rate = numMessages / elapsedSec;

        System.out.printf("Sent %,d messages in %.2f seconds (%.2f msg/sec)\n", numMessages, elapsedSec, rate);
    }
}
