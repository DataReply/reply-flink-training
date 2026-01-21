package com.reply.flink.training.solutions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import com.reply.flink.training.exercises.Exercise2_DataProcessing.Transaction;

import java.time.Duration;
import java.util.List;

public class Solution2_DataProcessing {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> raw = env.fromData(List.of(
                "User1,100,1000", "User2,50,2000", "User1,-10,3000", "User1,20,4000", "User2,50,5000"));

        DataStream<Transaction> transactions = raw.map(line -> {
                    String[] parts = line.split(",");
                    return new Transaction(parts[0], Double.parseDouble(parts[1]), Long.parseLong(parts[2]));
                }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                        .withTimestampAssigner((t, ts) -> t.timestamp))
                .filter(t -> t.amount > 0);

        transactions.keyBy(t -> t.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .sum("amount")
                .print();

        env.execute("Solution 2 - Data Processing");
    }
}
