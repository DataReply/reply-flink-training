package com.reply.flink.training.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Esercizio 3B: Arricchimento con Async I/O (Avanzato)
 * Obiettivo: Arricchire uno stream di Transaction con dati esterni usando il pattern Async I/O
 * per elevato throughput.
 * <p>
 * Questa è una variante AVANZATA dell'Esercizio 3 che dimostra il pattern Async I/O
 * per l'arricchimento con sistemi esterni (database, REST API, cache, ecc.)
 * <p>
 * TODO 1: Creare lo Stream di Transaction.
 * TODO 2: Implementare AsyncFunction per eseguire lookup asincrono.
 * TODO 3: Usare AsyncDataStream.unorderedWait() o orderedWait().
 * TODO 4: Configurare timeout e capacity.
 * <p>
 * Nota: Per la simulazione, puoi usare Thread.sleep() con CompletableFuture
 * per simulare una chiamata esterna asincrona. In produzione, usa librerie client asincrone
 * (Netty HTTP client, driver JDBC asincrono, client Redis asincrono, ecc.)
 */
public class Exercise3B_AsyncEnrichment {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: Implementare l'arricchimento asincrono
        // Suggerimento: Guarda l'API AsyncDataStream.unorderedWait()

        env.execute("Exercise 3B - Async Enrichment");
    }
}
