package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Esercizio 3: Arricchimento
 * Obiettivo: Arricchire uno stream di Transaction con UserLocation da uno stream
 * statico "lento" di stringhe (simulando un DB).
 * <p>
 * TODO 1: Creare lo Stream principale di Transaction.
 * TODO 2: Creare uno Stream di Rule/Enrichment (es., "User1->NY").
 * TODO 3: Connettere gli stream (Broadcast State o connect()).
 * TODO 4: In ProcessFunction, memorizzare la Rule nello state, applicarla alla Transaction.
 */
public class Exercise3_Enrichment {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Streams...

        env.execute("Exercise 3 - Enrichment");
    }
}
