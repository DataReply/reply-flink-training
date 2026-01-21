package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Esercizio 5: Gestione State & TTL
 * Obiettivo: Deduplicare uno stream di eventi usando Keyed State.
 * <p>
 * TODO 1: Stream di ID evento (alcuni duplicati).
 * TODO 2: KeyBy EventId.
 * TODO 3: Usare KeyedProcessFunction.
 * TODO 4: Definire ValueState<Boolean> "seen" con una TTL Config (es., 10 secondi).
 * TODO 5: Se visto, scartare. Se non visto, inoltrare e impostare seen=true.
 */
public class Exercise5_StateTTL {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute("Exercise 5 - State Management & TTL");
    }
}
