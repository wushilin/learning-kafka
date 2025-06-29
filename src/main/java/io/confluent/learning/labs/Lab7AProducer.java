package io.confluent.learning.labs;

import com.example.Animal;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

public class Lab7AProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("client.properties.secret"));
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer"); // NOTE on the serializer
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer"); // NOTE on the serializer

        int numMessages = 1000; // send 1000 messages
        Producer<String, Animal> producer = null; // TODO: Construct the producer here
        String topic = ""; // TODO: Set which topic to produce to. Create a new topic, do not use other topics
        Random rand = new Random();
        String[] types = new String[]{"Dog", "Cat", "Goat", "Sheep", "Cattle"}; // TODO: You can customize it if you want
        String[] owners = new String[]{"Steve", "Jacob", "Ryan", "Julie", "Jody", "Summer", "James"}; // TODO: Change?
        long idSequence = 0L;
        long startTime = System.nanoTime();
        for (int i = 0; i < numMessages; i++) {
            long id = idSequence++;
            String key = ""; // TODO initialize the key
            Animal value = null; // TODO initialize the value
            value.setId(id);
            // TODO call value.setX for each field, use some randomness?

            ProducerRecord<String, Animal> record = null; // TODO construct it
            // TODO: Send it
        }
        producer.flush();
        producer.close();

        long endTime = System.nanoTime();
        double elapsedSec = (endTime - startTime) / 1_000_000_000.0;
        double rate = numMessages / elapsedSec;

        System.out.printf("Sent %,d messages in %.2f seconds (%.2f msg/sec)\n", numMessages, elapsedSec, rate);
    }
}
