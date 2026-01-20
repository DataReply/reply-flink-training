package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Exercise 2: Data Processing
 * Goal: Parse a stream of Strings into POJOs, Filter, KeyBy, and Window.
 * <p>
 * Incoming Data schema: "UserId,Amount,Timestamp"
 * <p>
 * TODO 1: Map String to a Transaction POJO.
 * TODO 2: Filter out Transactions with amount < 0 (Fraud/Error).
 * TODO 3: KeyBy UserId.
 * TODO 4: Window (Tumbling Window of 5 seconds).
 * TODO 5: Sum the amounts per user/window.
 * TODO 6: Print results.
 */
public class Exercise2_DataProcessing {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream<String> raw = env.fromData(List.of(
        //         "User1,100,1000", "User2,50,2000", "User1,-10,3000", "User1,20,4000", "User2,50,5000"));

        // TODO implement pipeline

        env.execute("Exercise 2");
    }

    public static class Transaction {
        public String userId;
        public double amount;
        public long timestamp;

        public Transaction() {}
        public Transaction(String u, double a, long t) {
            this.userId = u;
            this.amount = a;
            this.timestamp = t;
        }

        @Override public String toString() {
            return "Transaction{userId='" + userId + "', amount=" + amount + ", timestamp=" + timestamp + "}";
        }
    }
}
