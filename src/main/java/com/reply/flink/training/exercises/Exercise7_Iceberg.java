package com.reply.flink.training.exercises;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Esercizio 7: Streaming verso Iceberg su S3
 * Obiettivo: Elaborare in streaming dati da Kafka (o datagen) e scrivere su tabelle Iceberg su AWS S3.
 * <p>
 * Scenario: Elaborare transazioni in streaming e memorizzarle in una tabella Iceberg
 * per analisi storica e query time-travel.
 * <p>
 * Setup dell'Ambiente:
 * - Bucket S3: Disponibile tramite variabile d'ambiente S3_WAREHOUSE_PATH
 * - Kafka: Disponibile tramite variabile d'ambiente KAFKA_BOOTSTRAP_SERVERS
 * - Autenticazione IAM: Automatica tramite instance profile (nessuna credenziale necessaria)
 * <p>
 * Attività:
 * TODO 1: Ottenere il path del warehouse S3 dalla variabile d'ambiente
 * TODO 2: Creare catalog Iceberg che punta a S3
 * TODO 3: Creare tabella source (Kafka o datagen per il testing)
 * TODO 4: Creare tabella sink Iceberg con schema appropriato
 * TODO 5: Streamare dati dalla source alla tabella Iceberg usando INSERT INTO
 * <p>
 * Suggerimenti:
 * - Usare Table API (TableEnvironment) per l'integrazione con Iceberg
 * - Iceberg catalog-type: 'hadoop'
 * - S3 FileIO: 'org.apache.iceberg.aws.s3.S3FileIO'
 * - Formato Parquet raccomandato per miglior compressione
 * - Usare colonne timestamp per capacità di time-travel
 */
public class Exercise7_Iceberg {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // TODO 1: Ottenere il path del warehouse S3 dall'ambiente
        String warehouse = System.getenv().getOrDefault("S3_WAREHOUSE_PATH",
                "s3a://flink-training-iceberg/warehouse");
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092");

        // TODO 2: Creare catalog Iceberg

        // TODO 3: Creare tabella source (usare datagen per testing, o Kafka)

        // Opzione B: Kafka (per scenario simile alla produzione)

        // TODO 4: Creare tabella sink Iceberg

        // TODO 5: Streamare dati verso Iceberg

        System.out.println("Esercizio 7 - Implementare streaming verso Iceberg su S3!");
        System.out.println("Path del warehouse disponibile a: " + System.getenv("S3_WAREHOUSE_PATH"));
        System.out.println("Kafka disponibile a: " + System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
    }
}
