package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Esercizio 6: Stream-Stream Join
 * Obiettivo: Join di 'Clicks' con 'Orders' dove l'Order avviene entro 1 minuto
 * dal Click.
 * <p>
 * TODO 1: Creare Click Stream (con Watermarks).
 * TODO 2: Creare Order Stream (con Watermarks).
 * TODO 3: KeyBy SessionId/UserId.
 * TODO 4: intervalJoin (tra -1 min e +0 min).
 * TODO 5: ProcessJoinFunction per produrre risultato combinato.
 */
public class Exercise6_Join {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.execute("Exercise 6 - Stream-Stream Join");
    }
}
