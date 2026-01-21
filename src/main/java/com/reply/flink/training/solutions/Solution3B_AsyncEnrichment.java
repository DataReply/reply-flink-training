package com.reply.flink.training.solutions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Solution 3B: Async I/O Enrichment
 * Demonstrates high-throughput enrichment using Async I/O pattern.
 */
public class Solution3B_AsyncEnrichment {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Transaction Stream
        DataStream<String> transactions = env.fromData(List.of(
                "Tx1,UserA,100",
                "Tx2,UserB,200",
                "Tx3,UserC,150",
                "Tx4,UserA,50",
                "Tx5,UserD,300"));

        // 2. Apply Async I/O for enrichment
        DataStream<String> enriched = AsyncDataStream.unorderedWait(
                transactions,
                new AsyncDatabaseLookup(),
                1000, // timeout in milliseconds
                TimeUnit.MILLISECONDS,
                100 // max concurrent requests
        );

        enriched.print();

        env.execute("Solution 3B - Async Enrichment");

        DataStream<String> text = env.fromData(List.of("hello", "world"));

        // Map: String -> Integer
        DataStream<Integer> lengths = text.map(String::length);

        // Filter: Mantiene numeri pari
        DataStream<Integer> evens = lengths.filter(i -> i % 2 == 0);
    }

    /**
     * Async function that simulates external database/API lookup.
     * In production, replace with actual async client (Netty, async JDBC, Redis
     * client, etc.)
     */
    public static class AsyncDatabaseLookup extends RichAsyncFunction<String, String> {

        // Simulated database (in production, this would be an async client)
        private transient Map<String, String> database;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize async client here
            database = new HashMap<>();
            database.put("UserA", "New York");
            database.put("UserB", "California");
            database.put("UserC", "Texas");
            database.put("UserD", "Florida");
        }

        @Override
        public void asyncInvoke(String transaction, ResultFuture<String> resultFuture) throws Exception {
            // Parse transaction
            String[] parts = transaction.split(",");
            String userId = parts[1];

            // Simulate async call with CompletableFuture
            CompletableFuture.supplyAsync(() -> {
                try {
                    // Simulate network latency
                    Thread.sleep(100);

                    // Lookup location
                    String location = database.getOrDefault(userId, "Unknown");

                    // Return enriched data
                    return transaction + ",Location=" + location;

                } catch (InterruptedException e) {
                    // Handle error
                    return transaction + ",Location=Error";
                }
            }).thenAccept(result -> {
                // Complete the future with the result
                resultFuture.complete(Collections.singleton(result));
            });
        }

        @Override
        public void timeout(String transaction, ResultFuture<String> resultFuture) throws Exception {
            // Handle timeout case
            System.err.println("Timeout for transaction: " + transaction);
            resultFuture.complete(Collections.singleton(transaction + ",Location=Timeout"));
        }

        @Override
        public void close() throws Exception {
            // Clean up async client resources here
            if (database != null) {
                database.clear();
            }
        }
    }
}
