package io.confluent.learning.solutions;

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
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        int numMessages = 1000; // send 1000 messages
        Producer<String, Animal> producer = new KafkaProducer<>(props);
        String topic = "test02";
        Random rand = new Random();
        String[] types = new String[]{"Dog", "Cat", "Goat", "Sheep", "Cattle"};
        String[] owners = new String[]{"Steve", "Jacob", "Ryan", "Julie", "Jody", "Summer", "James"};
        long idSequence = 0L;
        long startTime = System.nanoTime();
        for (int i = 0; i < numMessages; i++) {
            long id = idSequence++;
            String key = String.valueOf(id);
            Animal value = new Animal();
            value.setId(id);
            value.setName("Name of " + value.getId());
            value.setType(types[rand.nextInt(types.length)]);
            value.setNumberOfLegs(4);
            value.setOwnerName(owners[rand.nextInt(owners.length)]);

            ProducerRecord<String, Animal> record = new ProducerRecord<>(topic, key, value);
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
