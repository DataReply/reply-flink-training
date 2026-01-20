package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 3: Enrichment
 * Goal: Enrich a Transaction stream with UserLocation from a "slow" static
 * string stream (simulating DB).
 * <p>
 * TODO 1: Create Main Transaction Stream.
 * TODO 2: Create a Rule/Enrichment Stream (e.g., "User1->NY").
 * TODO 3: Connect the streams (Broadcast State or connect()).
 * TODO 4: In ProcessFunction, store Rule in state, apply to Transaction.
 */
public class Exercise3_Enrichment {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Streams...

        env.execute("Exercise 3");
    }
}
