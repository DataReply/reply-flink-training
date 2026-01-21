package com.reply.flink.training.solutions;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class Solution5_StateTTL {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> events = env.fromData(List.of("Event1", "Event2", "Event1", "Event3"));

        events
                .keyBy(id -> id)
                .process(new DeduplicationFunction())
                .print();

        env.execute("Solution 5 - State TTL");
    }

    public static class DeduplicationFunction extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Boolean> seenState;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Duration.ofSeconds(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Boolean.class);
            desc.enableTimeToLive(ttlConfig);

            seenState = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            if (seenState.value() == null) {
                out.collect(value);
                seenState.update(true);
            } else {
                // Drop duplicate
            }
        }
    }
}
