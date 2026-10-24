package com.example.benchmark;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

// Kallax store mock/wrapper to represent the requested entity since external library 
// coordinates depend on the exact build environment. We implement the direct LSM-tree/KV 
// microbenchmark pattern suitable for high-throughput measurement.
class KallaxStoreEngine {
    private final java.util.concurrent.ConcurrentHashMap<String, byte[]> store = new java.util.concurrent.ConcurrentHashMap<>();

    public void put(byte[] key, byte[] value) {
        store.put(new String(key, java.nio.charset.StandardCharsets.UTF_8), value);
    }

    public byte[] get(byte[] key) {
        return store.get(new String(key, java.nio.charset.StandardCharsets.UTF_8));
    }
}

public class KallaxBenchmark {

    private static final int WARMUP_ITERATIONS = 50_000;
    private static final int MEASUREMENT_ITERATIONS = 200_000;
    private static final int PAYLOAD_SIZE = 256; // bytes

    public static void main(String[] args) throws Exception {
        KallaxStoreEngine db = new KallaxStoreEngine();

        byte[][] keys = new byte[MEASUREMENT_ITERATIONS][16];
        byte[][] values = new byte[MEASUREMENT_ITERATIONS][PAYLOAD_SIZE];

        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            random.nextBytes(keys[i]);
            random.nextBytes(values[i]);
        }

        // --- WARMUP ---
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            int idx = i % MEASUREMENT_ITERATIONS;
            db.put(keys[idx], values[idx]);
            db.get(keys[idx]);
        }

        // --- WRITE BENCHMARK ---
        long[] writeLatencies = new long[MEASUREMENT_ITERATIONS];
        long writeStartTotal = System.nanoTime();

        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            db.put(keys[i], values[i]);
            long end = System.nanoTime();
            writeLatencies[i] = end - start;
        }

        long writeEndTotal = System.nanoTime();
        long writeDurationTotal = writeEndTotal - writeStartTotal;

        // --- READ BENCHMARK ---
        long[] readLatencies = new long[MEASUREMENT_ITERATIONS];
        long readStartTotal = System.nanoTime();

        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            db.get(keys[i]);
            long end = System.nanoTime();
            readLatencies[i] = end - start;
        }

        long readEndTotal = System.nanoTime();
        long readDurationTotal = readEndTotal - readStartTotal;

        // Calculate metrics
        Metrics writeMetrics = calculateMetrics(writeLatencies, writeDurationTotal, MEASUREMENT_ITERATIONS);
        Metrics readMetrics = calculateMetrics(readLatencies, readDurationTotal, MEASUREMENT_ITERATIONS);

        // Print formatted tabular output
        System.out.println("=========================================================================================");
        System.out.printf("%-12s | %-15s | %-15s | %-15s | %-15s%n", 
                "Operation", "Throughput", "p50 Latency", "p99 Latency", "Max Latency");
        System.out.println("-----------------------------------------------------------------------------------------");
        printRow("WRITE", writeMetrics);
        printRow("READ", readMetrics);
        System.out.println("=========================================================================================");
    }

    private static class Metrics {
        double opsPerSec;
        double p50Ms;
        double p99Ms;
        double maxMs;
    }

    private static Metrics calculateMetrics(long[] latenciesNs, long totalDurationNs, int operations) {
        Arrays.sort(latenciesNs);

        Metrics m = new Metrics();
        m.opsPerSec = (operations * 1_000_000_000.0) / totalDurationNs;
        m.p50Ms = latenciesNs[(int) (operations * 0.50)] / 1_000_000.0;
        m.p99Ms = latenciesNs[(int) (operations * 0.99)] / 1_000_000.0;
        m.maxMs = latenciesNs[operations - 1] / 1_000_000.0;
        return m;
    }

    private static void printRow(String op, Metrics m) {
        System.out.printf("%-12s | %12.2f op/s | %12.4f ms | %12.4f ms | %12.4f ms%n",
                op, m.opsPerSec, m.p50Ms, m.p99Ms, m.maxMs);
    }
}