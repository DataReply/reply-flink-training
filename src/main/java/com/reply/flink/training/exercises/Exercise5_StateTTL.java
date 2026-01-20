package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 5: State Management & TTL
 * Goal: Deduplicate a stream of events using Keyed State.
 * <p>
 * TODO 1: Stream of event IDs (some duplicates).
 * TODO 2: KeyBy EventId.
 * TODO 3: Use KeyedProcessFunction.
 * TODO 4: Define ValueState<Boolean> "seen" with a TTL Config (e.g., 10 seconds).
 * TODO 5: If seen, drop. If not seen, forward and set seen=true.
 */
public class Exercise5_StateTTL {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute("Exercise 5 - State Management & TTL");
    }
}
