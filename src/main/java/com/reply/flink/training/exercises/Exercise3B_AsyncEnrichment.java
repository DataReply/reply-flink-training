package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 3B: Async I/O Enrichment (Advanced)
 * Goal: Enrich a Transaction stream with external data using Async I/O pattern
 * for high throughput.
 * <p>
 * This is an ADVANCED variant of Exercise 3 demonstrating the Async I/O pattern
 * for enrichment with external systems (database, REST API, cache, etc.)
 * <p>
 * TODO 1: Create Transaction Stream.
 * TODO 2: Implement AsyncFunction to perform async lookup.
 * TODO 3: Use AsyncDataStream.unorderedWait() or orderedWait().
 * TODO 4: Configure timeout and capacity.
 * <p>
 * Note: For simulation, you can use Thread.sleep() with CompletableFuture
 * to simulate async external call. In production, use async client libraries
 * (Netty HTTP client, async JDBC driver, Redis async client, etc.)
 */
public class Exercise3B_AsyncEnrichment {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: Implement async enrichment
        // Hint: Look at AsyncDataStream.unorderedWait() API

        env.execute("Exercise 3B - Async Enrichment");
    }
}
