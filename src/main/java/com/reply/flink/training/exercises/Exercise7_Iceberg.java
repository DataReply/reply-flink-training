package com.reply.flink.training.exercises;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Exercise 7: Table API & Iceberg
 * Goal: Create a Table using SQL and write it to Iceberg.
 * <p>
 * Note: Requires local Hadoop/S3 path configuration or MinIO.
 * <p>
 * TODO 1: Create TableEnvironment.
 * TODO 2: Create a logical Table from a DataStream or via SQL DDL ("CREATE TABLE ...").
 * TODO 3: Create an Iceberg Catalog (via SQL or Java API).
 * TODO 4: INSERT INTO iceberg_table SELECT * FROM source_table.
 */
public class Exercise7_Iceberg {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // ... implementation
    }
}
