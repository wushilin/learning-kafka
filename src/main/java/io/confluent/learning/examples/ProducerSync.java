package io.confluent.learning.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

public class ProducerSync {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: java ProducerSync <topic> <num_messages>");
            System.exit(1);
        }

        String topic = args[0];
        int numMessages = Integer.parseInt(args[1]);

        Properties props = new Properties();
        props.load(new FileInputStream("client.properties.secret"));

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        Random rand = new Random();

        long startTime = System.nanoTime();

        for (int i = 0; i < numMessages; i++) {
            String key = String.format("%02d", rand.nextInt(100));
            User user = User.random();
            String value = objectMapper.writeValueAsString(user);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Delivered to %s-%d @ offset %d\n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }

        producer.flush();
        producer.close();

        long endTime = System.nanoTime();
        double elapsedSec = (endTime - startTime) / 1_000_000_000.0;
        double rate = numMessages / elapsedSec;

        System.out.printf("Sent %,d messages in %.2f seconds (%.2f msg/sec)\n", numMessages, elapsedSec, rate);
    }
}
