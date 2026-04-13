package com.github.koop.common.erasure;

import com.backblaze.erasure.ReedSolomon;

import java.io.*;
import java.util.Arrays;

/**
 * Stateless erasure coding utility using Reed-Solomon (6-of-9).
 *
 * Sharding:
 *   {@link #shard(InputStream, long)} splits a data stream into K+M shard streams.
 *   The first K shards are data shards; the remaining M are parity shards.
 *   Each returned InputStream begins with an 8-byte original-length prefix so the
 *   receiver can trim padding on reconstruction.
 *
 * Reconstruction:
 *   {@link #reconstruct(InputStream[], boolean[])} accepts up to K+M shard streams
 *   (false entries for missing shards) and returns the original data stream.
 *   At least K shards must be present.
 *
 * Concurrency note:
 *   The encoder runs in a single virtual thread and writes directly to each shard's
 *   PipedOutputStream. The pipe buffers are sized generously (4 * SHARD_SIZE each)
 *   so the encoder can run several stripes ahead of the consumers without blocking.
 *   The caller MUST drain all returned streams concurrently (e.g. one goroutine/thread
 *   per stream, or non-blocking I/O) — reading them sequentially will still deadlock
 *   once the pipe buffers fill up.
 */
public final class ErasureCoder {

    public static final int SHARD_SIZE = 1 << 20; // 1 MB per shard per stripe

    private ErasureCoder() {}

    // -------------------------------------------------------------------------
    // Sharding
    // -------------------------------------------------------------------------

    /**
     * Encodes {@code length} bytes from {@code data} into {@value TOTAL} shard streams.
     *
     * <p>The returned streams MUST be consumed concurrently. The encoder runs in a
     * background virtual thread; if the caller reads shard[0] to completion before
     * touching shard[1], the encoder will block on a full shard[1] pipe — deadlock.
     * In StorageWorker.put() the streams are forwarded to 9 independent socket writes
     * which already run concurrently, so this is safe there.
     *
     * @param data   source data; must supply exactly {@code length} bytes
     * @param length number of bytes to read from {@code data}
     * @return array of {@value TOTAL} InputStreams, index 0..K-1 = data, K..TOTAL-1 = parity
     */
public static InputStream[] shard(InputStream data, long length, int k, int n) throws IOException {
        if (data == null) throw new IllegalArgumentException("data is null");
        if (length < 0) throw new IllegalArgumentException("length < 0");

        int m = n - k;
        long numStripes = (length + (long) k * SHARD_SIZE - 1) / ((long) k * SHARD_SIZE);
        int pipeBuffer = (int) Math.min(8L * SHARD_SIZE, numStripes * SHARD_SIZE + 8);

        PipedOutputStream[] pos = new PipedOutputStream[n];
        PipedInputStream[] pis = new PipedInputStream[n];
        for (int i = 0; i < n; i++) {
            pos[i] = new PipedOutputStream();
            pis[i] = new PipedInputStream(pos[i], pipeBuffer);
        }

        Thread.startVirtualThread(() -> {
            try {
                byte[] lenBytes = new byte[8];
                writeLong(lenBytes, length);
                for (int i = 0; i < n; i++) pos[i].write(lenBytes);

                ReedSolomon rs = ReedSolomon.create(k, m);
                byte[] stripeBuf = new byte[k * SHARD_SIZE];
                byte[][] shards    = new byte[n][SHARD_SIZE];
                long remaining = length;

                while (remaining > 0) {
                    int want = (int) Math.min((long) stripeBuf.length, remaining);
                    readFully(data, stripeBuf, 0, want);
                    remaining -= want;

                    if (want < stripeBuf.length) {
                        Arrays.fill(stripeBuf, want, stripeBuf.length, (byte) 0);
                    }

                    for (int i = 0; i < k; i++) {
                        System.arraycopy(stripeBuf, i * SHARD_SIZE, shards[i], 0, SHARD_SIZE);
                    }
                    for (int i = k; i < n; i++) {
                        Arrays.fill(shards[i], (byte) 0);
                    }

                    rs.encodeParity(shards, 0, SHARD_SIZE);

                    for (int i = 0; i < n; i++) pos[i].write(shards[i], 0, SHARD_SIZE);
                }
                for (int i = 0; i < n; i++) pos[i].flush();
            } catch (IOException e) {
            } finally {
                for (PipedOutputStream p : pos) try { p.close(); } catch (IOException ignored) {}
            }
        });
        return pis;
    }

    // -------------------------------------------------------------------------
    // Reconstruction
    // -------------------------------------------------------------------------

    /**
     * Reconstructs the original data stream from at least {@value K} shard streams.
     *
     * @param shards  array of {@value TOTAL} shard InputStreams
     * @param present boolean mask; {@code false} = shard unavailable
     * @return InputStream yielding exactly the original bytes
     */
   public static InputStream reconstruct(InputStream[] shards, boolean[] present, int k, int n) throws IOException {
        if (shards == null || shards.length != n) throw new IllegalArgumentException("shards must have length " + n);
        if (present == null || present.length != n) throw new IllegalArgumentException("present must have length " + n);

        int count = 0;
        for (boolean b : present) if (b) count++;
        if (count < k) throw new IllegalArgumentException("need at least " + k + " shards, got " + count);

        int m = n - k;
        DataInputStream[] dis = new DataInputStream[n];
        for (int i = 0; i < n; i++) {
            if (present[i]) dis[i] = new DataInputStream(new BufferedInputStream(shards[i]));
        }

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 4 * k * SHARD_SIZE);
        final boolean[] pres = Arrays.copyOf(present, n);

        Thread.startVirtualThread(() -> {
            try (pos) {
                int first = -1;
                for (int i = 0; i < n; i++) if (pres[i]) { first = i; break; }

                long originalLength = dis[first].readLong();
                for (int i = 0; i < n; i++) {
                    if (pres[i] && i != first) dis[i].readLong();
                }

                ReedSolomon rs = ReedSolomon.create(k, m);
                byte[][] stripe = new byte[n][SHARD_SIZE];
                long remaining = originalLength;

                while (remaining > 0) {
                    for (int i = 0; i < n; i++) {
                        if (pres[i]) readFully(dis[i], stripe[i], 0, SHARD_SIZE);
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
            } finally {
                for (int i = 0; i < n; i++) {
                    if (dis[i] != null) try { dis[i].close(); } catch (IOException ignored) {}
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
            if (r < 0) throw new EOFException("Unexpected EOF");
            n += r;
        }
    }
}