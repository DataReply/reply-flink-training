package com.reply.flink.training.exercises;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Exercise 7: Streaming to Iceberg on S3
 * Goal: Stream process data from Kafka (or datagen) and sink to Iceberg tables on AWS S3.
 * <p>
 * Scenario: Process streaming transactions and store them in an Iceberg table
 * for historical analysis and time-travel queries.
 * <p>
 * Environment Setup:
 * - S3 bucket: Available via S3_WAREHOUSE_PATH environment variable
 * - Kafka: Available via KAFKA_BOOTSTRAP_SERVERS environment variable
 * - IAM authentication: Automatic via instance profile (no credentials needed)
 * <p>
 * Tasks:
 * TODO 1: Get S3 warehouse path from environment variable
 * TODO 2: Create Iceberg catalog pointing to S3
 * TODO 3: Create source table (Kafka or datagen for testing)
 * TODO 4: Create Iceberg sink table with proper schema
 * TODO 5: Stream data from source to Iceberg table using INSERT INTO
 * <p>
 * Hints:
 * - Use Table API (TableEnvironment) for Iceberg integration
 * - Iceberg catalog-type: 'hadoop'
 * - S3 FileIO: 'org.apache.iceberg.aws.s3.S3FileIO'
 * - Parquet format recommended for better compression
 * - Use timestamp columns for time-travel capabilities
 */
public class Exercise7_Iceberg {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // TODO 1: Get S3 warehouse path from environment
        String warehouse = System.getenv().getOrDefault("S3_WAREHOUSE_PATH",
                "s3a://flink-training-iceberg/warehouse");
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092");

        // TODO 2: Create Iceberg catalog

        // TODO 3: Create source table (use datagen for testing, or Kafka)

        // Option B: Kafka (for production-like scenario)

        // TODO 4: Create Iceberg sink table

        // TODO 5: Stream data to Iceberg

        System.out.println("Exercise 7 - Implement streaming to Iceberg on S3!");
        System.out.println("Warehouse path available at: " + System.getenv("S3_WAREHOUSE_PATH"));
        System.out.println("Kafka available at: " + System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
    }
}
