package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 4: Kafka Source & Sink
 * Goal: Read from Kafka topic 'input', process, write to 'output'.
 * <p>
 * TODO: Configure Kafka Source (bootstrap servers, group id).
 * TODO: Create DataStream from Kafka.
 * TODO: Write to Console or another Kafka topic.
 */
public class Exercise4_Kafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use environment variable for Kafka brokers (AWS MSK or localhost)
        String broker = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        System.out.println("Using Kafka bootstrap servers: " + broker);

        // TODO: Define properties for Kafka
        // Properties props = new Properties();
        // props.setProperty("bootstrap.servers", "localhost:9092");

        // TODO: Add Source

        // TODO: Execute
    }
}
