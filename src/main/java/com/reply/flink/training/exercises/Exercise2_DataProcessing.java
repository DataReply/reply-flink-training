package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Esercizio 2: Elaborazione Dati
 * Obiettivo: Parsare uno stream di Strings in POJO, Filter, KeyBy e Window.
 * <p>
 * Schema dei dati in arrivo: "UserId,Amount,Timestamp"
 * <p>
 * TODO 1: Mappare String a un POJO Transaction.
 * TODO 2: Filtrare le Transaction con amount < 0 (Frode/Errore).
 * TODO 3: KeyBy UserId.
 * TODO 4: Window (Tumbling Window di 5 secondi).
 * TODO 5: Sommare gli importi per utente/finestra.
 * TODO 6: Stampare i risultati.
 */
public class Exercise2_DataProcessing {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream<String> raw = env.fromData(List.of(
        //         "User1,100,1000", "User2,50,2000", "User1,-10,3000", "User1,20,4000", "User2,50,5000"));

        // TODO implementare la pipeline

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
