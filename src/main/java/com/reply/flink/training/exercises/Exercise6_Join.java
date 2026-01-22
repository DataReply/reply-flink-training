package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 6: Stream-Stream Join
 * Goal: Join 'Clicks' with 'Orders' where Order happens within 1 minute of
 * Click.
 * <p>
 * TODO 1: Create Click Stream (w/ Watermarks).
 * TODO 2: Create Order Stream (w/ Watermarks).
 * TODO 3: KeyBy SessionId/UserId.
 * TODO 4: intervalJoin (between -1 min and +0 min).
 * TODO 5: ProcessJoinFunction to output combined result.
 */
public class Exercise6_Join {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.execute("Exercise 6 - Stream-Stream Join");
    }
}
