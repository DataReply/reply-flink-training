package com.reply.flink.training.solutions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

// NOTE: For simplicity, assuming Tuple2/Tuple3 but simpler POJOs. Using Strings for brevity.
public class Solution6_Join {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Clicks: SessionId, Timestamp
        DataStream<String> clicks = env.fromData(List.of("S1,1000", "S2,2000"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> Long.parseLong(e.split(",")[1])));

        // 2. Orders: SessionId, Amount, Timestamp
        DataStream<String> orders = env.fromData(List.of("S1,50,1500", "S2,100,5000")) // S2 order is late (3s later)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner((e, t) -> Long.parseLong(e.split(",")[2])));

        clicks.keyBy(c -> c.split(",")[0])
                .intervalJoin(orders.keyBy(o -> o.split(",")[0]))
                .between(Duration.ofSeconds(0), Duration.ofSeconds(2)) // Order must happen within 2 seconds AFTER click
                .process(new ProcessJoinFunction<String, String, String>() {
                    @Override
                    public void processElement(String click, String order, Context ctx, Collector<String> out) {
                        out.collect("Joined: " + click + " with " + order);
                    }
                })
                .print();

        env.execute("Solution 6 - Interval Join");
    }
}
