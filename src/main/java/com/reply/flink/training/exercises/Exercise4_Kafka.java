package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Esercizio 4: Kafka Source & Sink
 * Obiettivo: Leggere dal topic Kafka 'input', elaborare, scrivere su 'output'.
 * <p>
 * TODO: Configurare Kafka Source (bootstrap servers, group id).
 * TODO: Creare DataStream da Kafka.
 * TODO: Scrivere su Console o su un altro topic Kafka.
 */
public class Exercise4_Kafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Usare variabile d'ambiente per i broker Kafka (AWS MSK o localhost)
        String broker = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        System.out.println("Utilizzo bootstrap servers Kafka: " + broker);

        // TODO: Definire le properties per Kafka
        // Properties props = new Properties();
        // props.setProperty("bootstrap.servers", "localhost:9092");
        
        // TODO: Aggiungere Source

        // TODO: Eseguire
    }
}
