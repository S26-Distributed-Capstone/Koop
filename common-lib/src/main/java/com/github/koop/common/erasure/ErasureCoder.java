package com.github.koop.common.erasure;

import com.backblaze.erasure.ReedSolomon;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Stateless erasure coding utility using Reed-Solomon with configurable
 * {@code k}-of-{@code n}
 * shard layouts.
 *
 * Sharding:
 * {@link #shard(InputStream, long, int, int)} splits a data stream into
 * {@code n} shard streams.
 * The first {@code k} shards are data shards; the remaining {@code n - k}
 * shards are parity shards.
 * Each returned {@link InputStream} begins with an 8-byte original-length
 * prefix so the receiver
 * can trim padding on reconstruction.
 *
 * Reconstruction:
 * {@link #reconstruct(InputStream[], boolean[], int, int)} accepts up to
 * {@code n} shard streams
 * ({@code false} entries for missing shards) and returns the original data
 * stream.
 * At least {@code k} shards must be present.
 */
public final class ErasureCoder {

    public static final int SHARD_SIZE = 1 << 20; // 1 MB per shard per stripe
    private static final Logger logger = LogManager.getLogger(ErasureCoder.class);

    private ErasureCoder() {
    }

    /**
     * A thread-pool-safe alternative to PipedInputStream/PipedOutputStream that
     * does not couple pipe validity to the thread identity of the reader.
     */
    private static class ThreadSafePipe {
        private final BlockingQueue<byte[]> queue;
        private volatile boolean closed = false;
        private volatile boolean broken = false;
        private volatile Throwable error = null;

        public ThreadSafePipe(int capacityChunks) {
            queue = new LinkedBlockingQueue<>(Math.max(1, capacityChunks));
        }

        /**
         * Signals an unrecoverable producer-side error. Subsequent reads on
         * {@link #in} will throw the supplied exception rather than returning
         * stale or partial data.
         */
        void fail(Throwable t) {
            error = t;
            queue.clear();
            queue.offer(new byte[0]); // wake any reader blocked on take()
        }

        /**
         * Enqueues {@code chunk} directly — the array is transferred by reference
         * with no copy. The caller must not modify {@code chunk} after this call.
         */
        void enqueue(byte[] chunk) throws IOException {
            if (broken)
                throw new IOException("Pipe broken by reader");
            if (chunk.length == 0)
                return;
            try {
                if (!queue.offer(chunk, 10, TimeUnit.SECONDS)) {
                    broken = true;
                    throw new IOException("Pipe write timed out - consumer is too slow");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }

        final InputStream in = new InputStream() {
            private byte[] current = null;
            private int pos = 0;
            private boolean eof = false;

            private void checkError() throws IOException {
                Throwable t = error;
                if (t != null)
                    throw t instanceof IOException ? (IOException) t : new IOException("Pipe error", t);
            }

            @Override
            public int read() throws IOException {
                byte[] b = new byte[1];
                int n = read(b, 0, 1);
                return n == -1 ? -1 : (b[0] & 0xFF);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                checkError();
                if (len == 0)
                    return 0;
                if (eof)
                    return -1;

                if (current == null || pos >= current.length) {
                    try {
                        current = queue.take();
                        pos = 0;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException();
                    }
                    checkError(); // re-check after waking; may have been unblocked by fail()
                }

                if (current.length == 0) { // empty array is the EOF marker
                    eof = true;
                    current = null;
                    return -1;
                }

                int toCopy = Math.min(len, current.length - pos);
                System.arraycopy(current, pos, b, off, toCopy);
                pos += toCopy;
                return toCopy;
            }

            @Override
            public void close() {
                broken = true;
                queue.clear();
                queue.offer(new byte[0]); // wake any reader blocked on take()
            }
        };

        final OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                write(new byte[] { (byte) b }, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                if (broken)
                    throw new IOException("Pipe broken by reader");
                if (len == 0)
                    return;
                byte[] chunk = new byte[len];
                System.arraycopy(b, off, chunk, 0, len);
                enqueue(chunk);
            }

            @Override
            public void flush() {
                // No-op. Chunks are immediately enqueued.
            }

            @Override
            public void close() throws IOException {
                if (!closed) {
                    closed = true;
                    if (error != null)
                        return; // fail() already signalled the consumer; skip EOF marker
                    try {
                        if (!queue.offer(new byte[0], 10, TimeUnit.SECONDS)) {
                            IOException e = new IOException(
                                    "Pipe close timed out - consumer did not drain within timeout");
                            fail(e); // unblock any reader blocked on take() before throwing
                            throw e;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException();
                    }
                }
            }
        };
    }

    // -------------------------------------------------------------------------
    // Sharding
    // -------------------------------------------------------------------------

    public static InputStream[] shard(InputStream data, long length, int k, int n) throws IOException {
        if (data == null)
            throw new IllegalArgumentException("data is null");
        if (length < 0)
            throw new IllegalArgumentException("length < 0");
        if (k == 0)
            throw new IllegalArgumentException("k must be > 0");
        if (n == 0)
            throw new IllegalArgumentException("n must be > 0");
        if (k > n)
            throw new IllegalArgumentException("k must be <= n");

        int m = n - k;
        long numStripes = (length + (long) k * SHARD_SIZE - 1) / ((long) k * SHARD_SIZE);
        // +2 accounts for the length-prefix chunk and the EOF marker so both fit
        // in the queue even before the consumer starts reading (avoids close() timeout
        // on zero- or single-stripe inputs).
        int capacityChunks = (int) Math.min(8L, numStripes + 2);

        ThreadSafePipe[] pipes = new ThreadSafePipe[n];
        InputStream[] pis = new InputStream[n];
        OutputStream[] pos = new OutputStream[n];

        for (int i = 0; i < n; i++) {
            pipes[i] = new ThreadSafePipe(capacityChunks);
            pos[i] = pipes[i].out;
            pis[i] = pipes[i].in;
        }

        Thread.startVirtualThread(() -> {
            try {
                byte[] lenBytes = new byte[8];
                writeLong(lenBytes, length);
                for (int i = 0; i < n; i++)
                    pos[i].write(lenBytes);

                ReedSolomon rs = ReedSolomon.create(k, m);
                byte[] stripeBuf = new byte[k * SHARD_SIZE];
                long remaining = length;
                boolean[] dead = new boolean[n];

                while (remaining > 0) {
                    int want = (int) Math.min((long) stripeBuf.length, remaining);
                    readFully(data, stripeBuf, 0, want);
                    remaining -= want;

                    if (want < stripeBuf.length) {
                        Arrays.fill(stripeBuf, want, stripeBuf.length, (byte) 0);
                    }

                    // Allocate fresh arrays per stripe so each array can be enqueued
                    // directly without an extra copy inside the pipe.
                    byte[][] shards = new byte[n][SHARD_SIZE];
                    for (int i = 0; i < k; i++) {
                        System.arraycopy(stripeBuf, i * SHARD_SIZE, shards[i], 0, SHARD_SIZE);
                    }
                    rs.encodeParity(shards, 0, SHARD_SIZE);

                    // Write sequentially — the pipes are async blocking queues so this
                    // does not stall the producer beyond normal backpressure.
                    for (int i = 0; i < n; i++) {
                        if (!dead[i]) {
                            try {
                                pipes[i].enqueue(shards[i]); // zero-copy hand-off
                            } catch (IOException e) {
                                dead[i] = true;
                                logger.warn("Pipe for shard " + i + " died. Halting writes.");
                            }
                        }
                    }
                }
                for (int i = 0; i < n; i++)
                    pos[i].flush();
            } catch (IOException e) {
                logger.error("Error in erasure coding thread", e);
                for (int i = 0; i < n; i++)
                    pipes[i].fail(e); // propagate to all consumers
            } finally {
                for (OutputStream p : pos)
                    try {
                        p.close();
                    } catch (IOException ignored) {
                    }
            }
        });
        return pis;
    }

    // -------------------------------------------------------------------------
    // Reconstruction
    // -------------------------------------------------------------------------

    public static InputStream reconstruct(InputStream[] shards, boolean[] present, int k, int n) throws IOException {
        if (shards == null || shards.length != n)
            throw new IllegalArgumentException("shards must have length " + n);
        if (present == null || present.length != n)
            throw new IllegalArgumentException("present must have length " + n);

        int count = 0;
        for (boolean b : present)
            if (b)
                count++;
        if (count < k)
            throw new IllegalArgumentException("need at least " + k + " shards, got " + count);

        int m = n - k;
        DataInputStream[] dis = new DataInputStream[n];
        for (int i = 0; i < n; i++) {
            if (present[i])
                dis[i] = new DataInputStream(new BufferedInputStream(shards[i]));
        }

        ThreadSafePipe pipe = new ThreadSafePipe(4 * k);
        InputStream pis = pipe.in;
        OutputStream pos = pipe.out;

        final boolean[] pres = Arrays.copyOf(present, n);

        Thread.startVirtualThread(() -> {
            try {
                int first = -1;
                for (int i = 0; i < n; i++)
                    if (pres[i]) {
                        first = i;
                        break;
                    }

                long originalLength = dis[first].readLong();
                for (int i = 0; i < n; i++) {
                    if (pres[i] && i != first)
                        dis[i].readLong();
                }

                ReedSolomon rs = ReedSolomon.create(k, m);
                byte[][] stripe = new byte[n][SHARD_SIZE];
                long remaining = originalLength;

                while (remaining > 0) {
                    for (int i = 0; i < n; i++) {
                        if (pres[i])
                            readFully(dis[i], stripe[i], 0, SHARD_SIZE);
                    }

                    rs.decodeMissing(stripe, pres, 0, SHARD_SIZE);

                    int toWrite = (int) Math.min((long) k * SHARD_SIZE, remaining);
                    int left = toWrite;
                    for (int i = 0; i < k && left > 0; i++) {
                        int nBytes = Math.min(SHARD_SIZE, left);
                        pos.write(stripe[i], 0, nBytes);
                        left -= nBytes;
                    }
                    remaining -= toWrite;
                }
                pos.flush();
            } catch (IOException e) {
                logger.error("Error in erasure reconstruction thread", e);
                pipe.fail(e); // propagate to consumer before closing
            } finally {
                try {
                    pos.close();
                } catch (IOException ignored) {
                }
                for (int i = 0; i < n; i++) {
                    if (dis[i] != null)
                        try {
                            dis[i].close();
                        } catch (IOException ignored) {
                        }
                }
            }
        });
        return pis;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void writeLong(byte[] buf, long v) {
        for (int i = 7; i >= 0; i--) {
            buf[i] = (byte) (v & 0xFF);
            v >>= 8;
        }
    }

    private static void readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int r = in.read(buf, off + n, len - n);
            if (r < 0)
                throw new EOFException("Unexpected EOF");
            n += r;
        }
    }
}
