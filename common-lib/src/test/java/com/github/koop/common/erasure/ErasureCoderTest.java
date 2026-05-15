package com.github.koop.common.erasure;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ErasureCoderTest {

    /**
     * Tests encoding and decoding of an object significantly larger than the
     * 1MB shard size and 8MB maximum pipe buffer, forcing multiple stripes
     * and requiring concurrent consumers to prevent deadlocks.
     */
    @Test
    public void testLargeObjectConcurrentEncodeDecode() throws Exception {
        int k = 4;
        int n = 6;
        int size = 25 * 1024 * 1024; // 25 MB
        byte[] original = new byte[size];
        new Random(42).nextBytes(original);

        InputStream[] shards = ErasureCoder.shard(new ByteArrayInputStream(original), size, k, n);
        byte[][] shardedData = new byte[n][];

        // Drain streams concurrently. Reading sequentially would result in a deadlock
        // when the pipe buffers fill up before the encoder completes.
        ExecutorService exec = Executors.newFixedThreadPool(n);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(exec.submit(() -> {
                shardedData[idx] = shards[idx].readAllBytes();
                return null;
            }));
        }

        // Wait for all shard streams to finish
        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        // Simulate missing shards (1 data, 1 parity)
        boolean[] present = new boolean[n];
        Arrays.fill(present, true);
        present[1] = false;
        present[5] = false;

        InputStream[] reconstructInputs = new InputStream[n];
        for (int i = 0; i < n; i++) {
            if (present[i]) {
                reconstructInputs[i] = new ByteArrayInputStream(shardedData[i]);
            }
        }

        InputStream reconstructedStream = ErasureCoder.reconstruct(reconstructInputs, present, k, n);
        byte[] reconstructed = reconstructedStream.readAllBytes();

        assertArrayEquals(original, reconstructed, "Reconstructed large data payload does not match the original byte array.");
    }

    /**
     * Tests resilience during the encoding phase if an output stream consumer dies or
     * closes the connection early. The encoder should trap the IOException, flag the stream
     * as dead, and continue writing to the surviving streams.
     */
    @Test
    public void testOutsGoDownDuringEncode() throws Exception {
        int k = 3;
        int n = 5;
        int size = 10 * 1024 * 1024; // 10 MB
        byte[] original = new byte[size];
        new Random(100).nextBytes(original);

        InputStream[] shards = ErasureCoder.shard(new ByteArrayInputStream(original), size, k, n);
        byte[][] shardedData = new byte[n][];

        ExecutorService exec = Executors.newFixedThreadPool(n);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(exec.submit(() -> {
                if (idx == 0 || idx == 4) {
                    // Simulate a network failure or downed node by closing the pipe early
                    shards[idx].close();
                    shardedData[idx] = null;
                } else {
                    shardedData[idx] = shards[idx].readAllBytes();
                }
                return null;
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        // Attempt reconstruction with the surviving streams (1, 2, 3)
        boolean[] present = new boolean[n];
        InputStream[] reconstructInputs = new InputStream[n];
        for (int i = 0; i < n; i++) {
            if (shardedData[i] != null) {
                present[i] = true;
                reconstructInputs[i] = new ByteArrayInputStream(shardedData[i]);
            }
        }

        InputStream reconstructedStream = ErasureCoder.reconstruct(reconstructInputs, present, k, n);
        byte[] reconstructed = reconstructedStream.readAllBytes();

        assertArrayEquals(original, reconstructed, "Data failed to reconstruct after output streams were interrupted during encode.");
    }

    @Test
    void testMultipleLargeObjectsAtOnceEncode() throws Exception{
        int k = 3;
        int n = 5;
        int size = 200 * 1024 * 1024; // 200 MB
        byte[] original = new byte[size];
        new Random(100).nextBytes(original);
        InputStream[] shards = ErasureCoder.shard(new ByteArrayInputStream(original), size, k, n);
        byte[][] shardedData = new byte[n][];

        ExecutorService exec = Executors.newFixedThreadPool(n);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(exec.submit(() -> {
                shardedData[idx] = shards[idx].readAllBytes();
                return null;
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        boolean[] present = new boolean[n];
        Arrays.fill(present, true);

        InputStream[] reconstructInputs = new InputStream[n];
        for (int i = 0; i < n; i++) {
            reconstructInputs[i] = new ByteArrayInputStream(shardedData[i]);
        }

        InputStream reconstructedStream = ErasureCoder.reconstruct(reconstructInputs, present, k, n);
        byte[] reconstructed = reconstructedStream.readAllBytes();

        assertArrayEquals(original, reconstructed, "Reconstructed large data payload does not match the original byte array.");
    }

   /**
     * Tests resilience against immediate pipe closures deterministically. 
     * By injecting a capturing Executor, the test prevents the producer thread 
     * from starting until after the main thread has explicitly closed the pipes, 
     * guaranteeing the prefix-write failure path is exercised.
     */
    @Test
    public void testImmediatePipeClosureSurvives() throws Exception {
        int m = 4;
        int n = 6;
        int size = 5 * 1024 * 1024; // 5 MB
        byte[] original = new byte[size];
        new Random(42).nextBytes(original);

        // 1. Create an executor that merely captures the task instead of running it
        AtomicReference<Runnable> producerTask = new AtomicReference<>();
        Executor capturingExecutor = producerTask::set;

        // 2. Call shard(). This creates the pipes, but the producer has NOT started.
        InputStream[] shards = ErasureCoder.shard(new ByteArrayInputStream(original), size, m, n, capturingExecutor);
        
        // 3. Deterministically close two shards to simulate instantaneous connection refused.
        shards[0].close();
        shards[1].close();

        byte[][] shardedData = new byte[n][];

        ExecutorService exec = Executors.newFixedThreadPool(n);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(exec.submit(() -> {
                if (idx != 0 && idx != 1) {
                    shardedData[idx] = shards[idx].readAllBytes();
                }
                return null;
            }));
        }

        // 4. Now that the pipes are closed and consumers are waiting, release the producer.
        Thread.startVirtualThread(producerTask.get());

        for (Future<?> f : futures) {
            f.get();
        }
        exec.shutdown();

        // Attempt reconstruction with the surviving streams (2, 3, 4, 5).
        boolean[] present = new boolean[n];
        InputStream[] reconstructInputs = new InputStream[n];
        for (int i = 0; i < n; i++) {
            if (shardedData[i] != null) {
                present[i] = true;
                reconstructInputs[i] = new ByteArrayInputStream(shardedData[i]);
            }
        }

        // Note: If you also add Executor injection to reconstruct(), update this call accordingly.
        InputStream reconstructedStream = ErasureCoder.reconstruct(reconstructInputs, present, m, n);
        byte[] reconstructed = reconstructedStream.readAllBytes();

        assertArrayEquals(original, reconstructed, "Data failed to reconstruct after immediate pipe closures.");
    }
}