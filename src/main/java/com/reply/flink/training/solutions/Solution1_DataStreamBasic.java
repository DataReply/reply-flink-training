package com.reply.flink.training.solutions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Reference Solution for Exercise 1.
 */
public class Solution1_DataStreamBasic {

    public static void main(String[] args) throws Exception {
        // 1. Get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Create a source
        DataStream<String> source = env.fromData(List.of("Hello", "Apache", "Flink", "Streaming"));

        // 3. Transformation
        DataStream<String> upperCase = source.map(String::toUpperCase);

        // 4. Sink
        upperCase.print();

        // 5. Execute
        env.execute("Solution 1 - Basic DataStream");
    }
}
