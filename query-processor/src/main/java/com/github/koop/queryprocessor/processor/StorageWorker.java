package com.github.koop.queryprocessor.processor;

import com.backblaze.erasure.ReedSolomon;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public final class StorageWorker {

    // Storage-node opcodes
    private static final int OP_STORE = 1;
    private static final int OP_DELETE = 2;
    private static final int OP_READ = 6;

    // Erasure coding parameters (tolerate 3 failures)
    private static final int K = 6;
    private static final int M = 3;
    private static final int TOTAL = K + M;

    // Stripe block size
    private static final int SHARD_SIZE = 1 << 20; // 1MB

    private static final int CONNECT_TIMEOUT_MS = 3000;

    // Each set contains TOTAL nodes, node index (0..8) implies shard index
    private final List<InetSocketAddress> set1;
    private final List<InetSocketAddress> set2;
    private final List<InetSocketAddress> set3;

    public StorageWorker(List<InetSocketAddress> set1,
                         List<InetSocketAddress> set2,
                         List<InetSocketAddress> set3) {
        if (set1.size() != TOTAL || set2.size() != TOTAL || set3.size() != TOTAL) {
            throw new IllegalArgumentException("Each set must have exactly " + TOTAL + " nodes");
        }
        this.set1 = set1;
        this.set2 = set2;
        this.set3 = set3;
    }

    public boolean put(UUID requestID,
                       String bucket,
                       String key,
                       long length,
                       InputStream data) throws IOException {

        // ---- Basic validation ----
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null) throw new IllegalArgumentException("bucket is null");
        if (key == null) throw new IllegalArgumentException("key is null");
        if (data == null) throw new IllegalArgumentException("data is null");
        if (length < 0) throw new IllegalArgumentException("length < 0");

        // Storage key includes bucket prefix
        String storageKey = bucket + "-" + key;

        // Routing gives us:
        //  routing[0] = which erasure set (1,2,3)
        //  routing[1] = partition (CRC32 % 100)
        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        // Choose the correct erasure set (list of 9 node addresses)
        List<InetSocketAddress> nodes = nodesForKey(setNum);

        // ---- Stripe math ----
        // Each stripe holds K * SHARD_SIZE bytes of real data
        long stripeDataBytes = (long) K * SHARD_SIZE;

        // Number of stripes required to store full object
        long numStripes = (length + stripeDataBytes - 1) / stripeDataBytes;

        // Each shard stream will contain:
        //   8 bytes originalLength
        //   numStripes blocks of SHARD_SIZE
        long shardLengthBytes = 8L + numStripes * SHARD_SIZE;

        Socket[] sockets = new Socket[TOTAL];
        DataOutputStream[] outs = new DataOutputStream[TOTAL];
        InputStream[] ins = new InputStream[TOTAL];

        try {
            // ---- Phase 1: Connect to all shard nodes ----
            // Each node corresponds to one shard index (0..8)
            for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {

                Socket s = new Socket();
                s.connect(nodes.get(shardIndex), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);

                sockets[shardIndex] = s;
                outs[shardIndex] = new DataOutputStream(
                        new BufferedOutputStream(s.getOutputStream()));
                ins[shardIndex] = new BufferedInputStream(s.getInputStream());

                // ---- Send STORE header ----
                // Format:
                //   opcode (1)
                //   requestId (string) (length first)
                //   partition (int mod value)
                //   key (string) (length first)
                //   data stream (rest of connection)
                outs[shardIndex].writeInt(OP_STORE);
                writeString(outs[shardIndex], requestID.toString());
                outs[shardIndex].writeInt(partition);
                writeString(outs[shardIndex], storageKey);
                outs[shardIndex].flush();
            }

            // ---- Phase 2: Send 8-byte original length to each shard ----
            // This is embedded in the shard data stream.
            // All shards contain the same original object length.
            for (int i = 0; i < TOTAL; i++) {
                outs[i].writeLong(length);
            }

            ReedSolomon rs = ReedSolomon.create(K, M);

            byte[] stripeBuf = new byte[K * SHARD_SIZE];
            byte[][] shards = new byte[TOTAL][SHARD_SIZE];

            long remaining = length;

            // ---- Phase 3: Read stripes, encode parity, stream to nodes ----
            while (remaining > 0) {

                // Read up to one full stripe of real data
                int want = (int) Math.min((long) stripeBuf.length, remaining);
                readFully(data, stripeBuf, 0, want);
                remaining -= want;

                // If this is the last partial stripe, zero-pad to full size
                if (want < stripeBuf.length) {
                    Arrays.fill(stripeBuf, want, stripeBuf.length, (byte) 0);
                }

                // Copy stripe data into first K shards
                for (int i = 0; i < K; i++) {
                    System.arraycopy(stripeBuf,
                            i * SHARD_SIZE,
                            shards[i],
                            0,
                            SHARD_SIZE);
                }

                // Clear parity shards before computing
                for (int i = K; i < TOTAL; i++) {
                    Arrays.fill(shards[i], (byte) 0);
                }

                // Compute parity shards
                rs.encodeParity(shards, 0, SHARD_SIZE);

                // Write each shard block to its node
                for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {
                    outs[shardIndex].write(shards[shardIndex], 0, SHARD_SIZE);
                }
            }

            // Flush all shard streams
            for (int i = 0; i < TOTAL; i++) {
                outs[i].flush();


                // Signal end-of-stream using TCP half-close.
                sockets[i].shutdownOutput();
            }

            // ---- Phase 4: Wait for node acknowledgements ----
            for (int i = 0; i < TOTAL; i++) {
                int ack = ins[i].read();
                if (ack != 1) return false;
            }

            return true;

        } finally {
            for (Socket s : sockets)
                if (s != null) closeQuietly(s);
        }
    }


    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if(requestID == null) throw new IllegalArgumentException("requestID is null");
        if(bucket == null) throw new IllegalArgumentException("bucket is null");
        if(key == null) throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        // pos = writer side
        PipedOutputStream pos = new PipedOutputStream();

        // pis = reader side
        // 256KB internal buffer prevents tiny-buffer performance problems
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        // Reconstruction happens in a virtual thread
        Thread.startVirtualThread(() -> {
            try (pos) {
                streamReconstruct(partition, storageKey, nodes, pos);
            } catch (Exception e) {
                try { pos.close(); } catch (IOException ignored) {}
            }
        });

        // Caller reads reconstructed object from this InputStream
        return pis;
    }


    public boolean delete(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null) throw new IllegalArgumentException("bucket is null");
        if (key == null) throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;
        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        Socket[] sockets = new Socket[TOTAL];
        DataOutputStream[] outs = new DataOutputStream[TOTAL];
        InputStream[] ins = new InputStream[TOTAL];

        try {
            for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {
                Socket s = new Socket();
                s.connect(nodes.get(shardIndex), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);
                sockets[shardIndex] = s;

                outs[shardIndex] = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ins[shardIndex] = new BufferedInputStream(s.getInputStream());

                // DELETE request:
                // opcode=2, partition int, key string
                outs[shardIndex].writeInt(OP_DELETE);
                outs[shardIndex].writeInt(partition);
                writeString(outs[shardIndex], storageKey);
                outs[shardIndex].flush();

                try { s.shutdownOutput(); } catch (IOException ignored) {}
            }

            //1 byte from node
            for (int i = 0; i < TOTAL; i++) {
                int ack = ins[i].read();
                if (ack != 1) return false;
            }
            return true;

        } catch (IOException e) {
            return false;
        } finally {
            for (Socket s : sockets) if (s != null) closeQuietly(s);
        }
    }

    // ---------------- internal reconstruct ----------------

    private void streamReconstruct(int partition, String storageKey, List<InetSocketAddress> nodes, OutputStream out) throws IOException {

        // Each shard connection corresponds to one shard index (0..8)
        Socket[] sockets = new Socket[TOTAL];
        DataInputStream[] ins = new DataInputStream[TOTAL];
        boolean[] present = new boolean[TOTAL];

        long shardLength = -1;

        // ---- Phase 1: Request shards from all nodes ----
        for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {

            try {
                Socket s = new Socket();
                s.connect(nodes.get(shardIndex), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);

                sockets[shardIndex] = s;

                DataOutputStream nodeOut = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ins[shardIndex] = new DataInputStream(new BufferedInputStream(s.getInputStream()));

                // READ request format:
                //   opcode (6)
                //   partition (mod value)
                //   key
                nodeOut.writeInt(OP_READ);
                nodeOut.writeInt(partition);
                writeString(nodeOut, storageKey);
                nodeOut.flush();
                s.shutdownOutput(); // tell node we're done sending

                // Node response format:
                //   long totalLength
                //   1 byte success flag
                //   shard bytes...
                long respLen = ins[shardIndex].readLong();
                if (respLen < 1) {
                    present[shardIndex] = false;
                    continue;
                }

                int ok = ins[shardIndex].readUnsignedByte();
                if (ok == 0) {
                    present[shardIndex] = false;
                    continue;
                }

                long sl = respLen - 1;
                present[shardIndex] = true;

                // Ensure all shards agree on shardLength
                if (shardLength < 0) shardLength = sl;
                else if (shardLength != sl) present[shardIndex] = false;

            } catch (IOException e) {
                present[shardIndex] = false;
            }
        }

        // Must have at least K shards to reconstruct
        int have = 0;
        for (boolean b : present) if (b) have++;
        if (have < K) throw new IOException("not enough shards");

        // ---- Phase 2: Read original object length from shards ----
        long originalLength = -1;

        for (int i = 0; i < TOTAL; i++) {
            if (!present[i] || ins[i] == null) continue;

            long L = ins[i].readLong(); // first 8 bytes of shard stream
            if (originalLength < 0) originalLength = L;
            else if (originalLength != L) present[i] = false;
        }

        // ---- Phase 3: Decode stripe-by-stripe ----
        long numStripes = (shardLength - 8) / SHARD_SIZE;

        ReedSolomon rs = ReedSolomon.create(K, M);

        byte[][] shards = new byte[TOTAL][SHARD_SIZE];
        boolean[] stripePresent = Arrays.copyOf(present, present.length);

        long remaining = originalLength;

        for (long stripe = 0; stripe < numStripes && remaining > 0; stripe++) {

            int presentCount = 0;

            // Read one block from each shard
            for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {

                if (!stripePresent[shardIndex] || ins[shardIndex] == null) {
                    Arrays.fill(shards[shardIndex], (byte) 0);
                    continue;
                }

                boolean ok = readExactly(ins[shardIndex], shards[shardIndex], 0, SHARD_SIZE);

                if (ok) presentCount++;
                else stripePresent[shardIndex] = false;
            }

            if (presentCount < K)
                throw new IOException("lost too many shards mid-stream");

            // Reconstruct missing shards
            rs.decodeMissing(shards, stripePresent, 0, SHARD_SIZE);

            // Write reconstructed real data to caller stream
            for (int i = 0; i < K && remaining > 0; i++) {
                int toWrite = (int) Math.min((long) SHARD_SIZE, remaining);
                out.write(shards[i], 0, toWrite);
                remaining -= toWrite;
            }
        }

        out.flush();
    }


    // ---------------- helpers ----------------

    private List<InetSocketAddress> nodesForKey(int setNum) {
        return switch (setNum) {
            case 1 -> set1;
            case 2 -> set2;
            case 3 -> set3;
            default -> throw new IllegalArgumentException("bad setNum " + setNum);
        };
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static void readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int r = in.read(buf, off + total, len - total);
            if (r == -1) throw new EOFException("Unexpected EOF");
            total += r;
        }
    }

    private static boolean readExactly(InputStream in, byte[] buf, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int r = in.read(buf, off + total, len - total);
            if (r == -1) return false;
            total += r;
        }
        return true;
    }

    private static void closeQuietly(Socket s) {
        if (s != null) try { s.close(); } catch (IOException ignored) {}
    }
}
