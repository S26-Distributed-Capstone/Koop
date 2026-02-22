package com.github.koop.queryprocessor.processor;

import com.backblaze.erasure.ReedSolomon;
import com.github.koop.common.messages.InputStreamMessageReader;
import com.github.koop.common.messages.MessageBuilder;
import com.github.koop.common.messages.Opcode;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public final class StorageWorker {

    // Erasure coding parameters
    private static final int K = 6;
    private static final int M = 3;
    private static final int TOTAL = K + M;

    // Stripe block size
    private static final int SHARD_SIZE = 1 << 20; // 1MB

    private static final int CONNECT_TIMEOUT_MS = 3000;

    private final List<InetSocketAddress> set1;
    private final List<InetSocketAddress> set2;
    private final List<InetSocketAddress> set3;

    public StorageWorker() {
        set1 = List.of();
        set2 = List.of();
        set3 = List.of();
    }

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

    public boolean put(UUID requestID, String bucket, String key, long length, InputStream data) throws IOException {

        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null) throw new IllegalArgumentException("bucket is null");
        if (key == null) throw new IllegalArgumentException("key is null");
        if (data == null) throw new IllegalArgumentException("data is null");
        if (length < 0) throw new IllegalArgumentException("length < 0");

        String storageKey = bucket + "-" + key;

        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        // Stripe math
        long stripeDataBytes = (long) K * SHARD_SIZE;
        long numStripes = (length + stripeDataBytes - 1) / stripeDataBytes;

        // Payload we stream after header = [8 bytes originalLength] + [numStripes * SHARD_SIZE]
        long payloadLength = 8L + numStripes * SHARD_SIZE;

        Socket[] sockets = new Socket[TOTAL];
        DataOutputStream[] outs = new DataOutputStream[TOTAL];
        InputStream[] ins = new InputStream[TOTAL];

        try {
            // Connect and send STORE headers
            for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {

                Socket s = new Socket();
                s.connect(nodes.get(shardIndex), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);

                sockets[shardIndex] = s;
                outs[shardIndex] = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ins[shardIndex]  = new BufferedInputStream(s.getInputStream());

                MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
                putMsg.writeString(requestID.toString());
                putMsg.writeInt(partition);
                putMsg.writeString(storageKey);
                
                // Hack to increase the total Frame Length inside the builder 
                // so we can stream the payload manually after the header
                putMsg.writeLargePayload(payloadLength, (InputStream) null); 
                putMsg.writeToOutputStream(outs[shardIndex]);
            }

            // Send original length into the payload stream (counts toward payloadLength)
            for (int i = 0; i < TOTAL; i++) {
                outs[i].writeLong(length);
            }

            ReedSolomon rs = ReedSolomon.create(K, M);

            byte[] stripeBuf = new byte[K * SHARD_SIZE];
            byte[][] shards = new byte[TOTAL][SHARD_SIZE];

            long remaining = length;

            while (remaining > 0) {
                int want = (int) Math.min((long) stripeBuf.length, remaining);
                readFully(data, stripeBuf, 0, want);
                remaining -= want;

                if (want < stripeBuf.length) {
                    Arrays.fill(stripeBuf, want, stripeBuf.length, (byte) 0);
                }

                for (int i = 0; i < K; i++) {
                    System.arraycopy(stripeBuf, i * SHARD_SIZE, shards[i], 0, SHARD_SIZE);
                }

                for (int i = K; i < TOTAL; i++) {
                    Arrays.fill(shards[i], (byte) 0);
                }

                rs.encodeParity(shards, 0, SHARD_SIZE);

                for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {
                    outs[shardIndex].write(shards[shardIndex], 0, SHARD_SIZE);
                }
            }

            for (int i = 0; i < TOTAL; i++) {
                outs[i].flush();
                sockets[i].shutdownOutput(); // end payload stream cleanly
            }

            // Server responds with Success message
            for (int i = 0; i < TOTAL; i++) {
                InputStreamMessageReader reader = new InputStreamMessageReader(ins[i]);
                if (reader.getOpcode() != Opcode.SN_PUT.getCode()) return false;
                if (reader.readByte() != 1) return false;
            }

            return true;

        } catch (IOException e) {
            return false;
        } finally {
            for (Socket s : sockets) if (s != null) closeQuietly(s);
        }
    }

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null) throw new IllegalArgumentException("requestID is null");
        if (bucket == null) throw new IllegalArgumentException("bucket is null");
        if (key == null) throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;

        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        Thread.startVirtualThread(() -> {
            try (pos) {
                streamReconstruct(partition, storageKey, nodes, pos);
            } catch (Exception e) {
                try { pos.close(); } catch (IOException ignored) {}
            }
        });

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
                ins[shardIndex]  = new BufferedInputStream(s.getInputStream());

                MessageBuilder delMsg = new MessageBuilder(Opcode.SN_DELETE);
                delMsg.writeInt(partition);
                delMsg.writeString(storageKey);
                delMsg.writeToOutputStream(outs[shardIndex]);
                outs[shardIndex].flush();
                s.shutdownOutput();
            }

            // Server responds with Success message
            for (int i = 0; i < TOTAL; i++) {
                InputStreamMessageReader reader = new InputStreamMessageReader(ins[i]);
                if (reader.getOpcode() != Opcode.SN_DELETE.getCode()) return false;
                if (reader.readByte() != 1) return false;
            }
            return true;

        } catch (IOException e) {
            return false;
        } finally {
            for (Socket s : sockets) if (s != null) closeQuietly(s);
        }
    }

    private void streamReconstruct(int partition, String storageKey, List<InetSocketAddress> nodes, OutputStream out) throws IOException {

        Socket[] sockets = new Socket[TOTAL];
        DataInputStream[] ins = new DataInputStream[TOTAL];
        boolean[] present = new boolean[TOTAL];

        // Request shards
        for (int shardIndex = 0; shardIndex < TOTAL; shardIndex++) {
            try {
                Socket s = new Socket();
                s.connect(nodes.get(shardIndex), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);

                sockets[shardIndex] = s;

                DataOutputStream nodeOut = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ins[shardIndex] = new DataInputStream(new BufferedInputStream(s.getInputStream()));

                MessageBuilder getMsg = new MessageBuilder(Opcode.SN_GET);
                getMsg.writeInt(partition);
                getMsg.writeString(storageKey);
                getMsg.writeToOutputStream(nodeOut);
                nodeOut.flush();
                s.shutdownOutput();

                InputStreamMessageReader reader = new InputStreamMessageReader(ins[shardIndex]);
                if (reader.getOpcode() != Opcode.SN_GET.getCode()) { present[shardIndex] = false; continue; }

                int ok = reader.readByte();
                if (ok == 0) { present[shardIndex] = false; continue; }

                present[shardIndex] = true;

            } catch (IOException e) {
                present[shardIndex] = false;
            }
        }

        int presentCount = 0;
        for (boolean b : present) if (b) presentCount++;
        if (presentCount < K) throw new IOException("lost too many shards mid-stream");

        // Each shard begins with 8 bytes original length
        int first = -1;
        for (int i = 0; i < TOTAL; i++) {
            if (present[i]) { first = i; break; }
        }
        long originalLength = ins[first].readLong();
        for (int i = 0; i < TOTAL; i++) {
            if (present[i] && i != first) ins[i].readLong();
        }

        ReedSolomon rs = ReedSolomon.create(K, M);

        byte[][] shards = new byte[TOTAL][SHARD_SIZE];
        boolean[] shardPresent = Arrays.copyOf(present, present.length);

        long remaining = originalLength;
        while (remaining > 0) {

            for (int i = 0; i < TOTAL; i++) {
                if (!shardPresent[i]) continue;
                readFully(ins[i], shards[i], 0, SHARD_SIZE);
            }

            rs.decodeMissing(shards, shardPresent, 0, SHARD_SIZE);

            int toWrite = (int) Math.min((long) K * SHARD_SIZE, remaining);

            int left = toWrite;
            for (int i = 0; i < K && left > 0; i++) {
                int n = Math.min(SHARD_SIZE, left);
                out.write(shards[i], 0, n);
                left -= n;
            }

            remaining -= toWrite;
        }

        out.flush();

        for (Socket s : sockets) if (s != null) closeQuietly(s);
    }

    private List<InetSocketAddress> nodesForKey(int setNum) {
        return switch (setNum) {
            case 1 -> set1;
            case 2 -> set2;
            default -> set3;
        };
    }

    private static void readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int r = in.read(buf, off + n, len - n);
            if (r < 0) throw new EOFException("Unexpected EOF");
            n += r;
        }
    }

    private static void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) {}
    }
}