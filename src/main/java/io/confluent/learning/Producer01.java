package io.confluent.learning;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

        import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Producer01 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: java MainProducer <topic> <num_messages>");
            System.exit(1);
        }

        String topic = args[0];
        int numMessages = Integer.parseInt(args[1]);

        Properties props = new Properties();
        props.load(new FileInputStream("client.properties"));

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = ne

