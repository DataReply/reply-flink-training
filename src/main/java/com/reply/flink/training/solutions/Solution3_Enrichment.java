package com.reply.flink.training.solutions;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class Solution3_Enrichment {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Transaction Stream
        DataStream<String> transactions = env.fromData(List.of(
                "Tx1,UserA,100", "Tx2,UserB,200", "Tx3,UserA,50"))
                .map(tx -> { Thread.sleep(2000); return tx; });

        // 2. Rule/Metadata Stream (User -> Location)
        DataStream<String> rules = env.fromData(List.of(
                "UserA,NY", "UserB,CA"));

        // 3. Define State Descriptor for Broadcast
        MapStateDescriptor<String, String> locationStateDescriptor = new MapStateDescriptor<>("UserLocation",
                Types.STRING, Types.STRING);

        // 4. Broadcast the rules
        BroadcastStream<String> broadcastRules = rules.broadcast(locationStateDescriptor);

        // 5. Connect and Process
        transactions
                .connect(broadcastRules)
                .process(new BroadcastProcessFunction<String, String, String>() {

                    @Override
                    public void processBroadcastElement(String rule, Context ctx, Collector<String> out)
                            throws Exception {
                        String[] parts = rule.split(",");
                        // Update State
                        BroadcastState<String, String> state = ctx.getBroadcastState(locationStateDescriptor);
                        state.put(parts[0], parts[1]);
                    }

                    @Override
                    public void processElement(String tx, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        String[] parts = tx.split(",");
                        String userId = parts[1];
                        // Read State
                        String location = ctx.getBroadcastState(locationStateDescriptor).get(userId);
                        out.collect(tx + ",Location=" + (location != null ? location : "Unknown"));
                    }
                })
                .print();

        env.execute("Solution 3 - Enrichment");
    }
}
