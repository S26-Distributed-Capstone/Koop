package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.InputStreamMessageReader;
import com.github.koop.common.messages.MessageBuilder;
import com.github.koop.common.messages.Opcode;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.koop.common.erasure.ErasureCoder.TOTAL;

public final class StorageWorker {

    private static final int CONNECT_TIMEOUT_MS = 3000;

    private final List<InetSocketAddress> set1;
    private final List<InetSocketAddress> set2;
    private final List<InetSocketAddress> set3;

    public StorageWorker() {
        set1 = List.of();
        set2 = List.of();
        set3 = List.of();
    }

    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2, List<InetSocketAddress> set3) {
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

        int[] routing   = ErasureRouting.setForKey(storageKey);
        int   setNum    = routing[0];
        int   partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        // Shard the incoming stream. Each InputStream carries an 8-byte length prefix
        // followed by ceil(length / K*SHARD_SIZE) * SHARD_SIZE bytes of shard data.
        InputStream[] shardStreams = ErasureCoder.shard(data, length);

        long stripeDataBytes = (long) ErasureCoder.K * ErasureCoder.SHARD_SIZE;
        long numStripes = (length + stripeDataBytes - 1) / stripeDataBytes;
        long shardPayload = 8L + numStripes * ErasureCoder.SHARD_SIZE;

        Socket[] sockets = new Socket[TOTAL];
        DataOutputStream[] outs = new DataOutputStream[TOTAL];
        InputStream[] ins = new InputStream[TOTAL];

        try {
            // Connect and send STORE headers
            for (int i = 0; i < TOTAL; i++) {
                Socket s = new Socket();
                s.connect(nodes.get(i), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);

                sockets[i] = s;
                outs[i] = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ins[i] = new BufferedInputStream(s.getInputStream());

                MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
                putMsg.writeString(requestID.toString());
                putMsg.writeInt(partition);
                putMsg.writeString(storageKey);
                putMsg.writeLargePayload(shardPayload, (InputStream) null);
                putMsg.writeToOutputStream(outs[i]);
            }

            // IMPORTANT: all shard streams must be drained concurrently.
            // The ErasureCodec encoder runs in one virtual thread writing to 9 pipes.
            // If we drain shards sequentially, shard[1..8]'s pipe buffers fill up while
            // we're still reading shard[0], causing the encoder to block → deadlock.
            // One virtual thread per shard fixes this.
            AtomicBoolean anyFailed = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(TOTAL);

            for (int i = 0; i < TOTAL; i++) {
                final int idx = i;
                final InputStream src = shardStreams[i];
                final DataOutputStream dst = outs[i];
                final Socket sock = sockets[i];

                Thread.startVirtualThread(() -> {
                    try {
                        byte[] buf = new byte[64 * 1024];
                        int n;
                        while ((n = src.read(buf)) != -1) {
                            dst.write(buf, 0, n);
                        }
                        dst.flush();
                        sock.shutdownOutput();
                    } catch (IOException e) {
                        anyFailed.set(true);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            if (anyFailed.get()) return false;

            // Wait for success acknowledgements from all nodes
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
        PipedInputStream  pis = new PipedInputStream(pos, 256 * 1024);

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
            for (int i = 0; i < TOTAL; i++) {
                Socket s = new Socket();
                s.connect(nodes.get(i), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);
                sockets[i] = s;

                outs[i] = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ins[i] = new BufferedInputStream(s.getInputStream());

                MessageBuilder delMsg = new MessageBuilder(Opcode.SN_DELETE);
                delMsg.writeInt(partition);
                delMsg.writeString(storageKey);
                delMsg.writeToOutputStream(outs[i]);
                outs[i].flush();
                s.shutdownOutput();
            }

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

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void streamReconstruct(int partition, String storageKey, List<InetSocketAddress> nodes, OutputStream out) throws IOException {

        Socket[] sockets = new Socket[TOTAL];
        InputStream[] ins = new InputStream[TOTAL];
        boolean[] present = new boolean[TOTAL];

        for (int i = 0; i < TOTAL; i++) {
            try {
                Socket s = new Socket();
                s.connect(nodes.get(i), CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);
                sockets[i] = s;

                DataOutputStream nodeOut = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));

                MessageBuilder getMsg = new MessageBuilder(Opcode.SN_GET);
                getMsg.writeInt(partition);
                getMsg.writeString(storageKey);
                getMsg.writeToOutputStream(nodeOut);
                nodeOut.flush();
                s.shutdownOutput();

                ins[i] = new BufferedInputStream(s.getInputStream());
                InputStreamMessageReader reader = new InputStreamMessageReader(ins[i]);
                if (reader.getOpcode() != Opcode.SN_GET.getCode() || reader.readByte() == 0) {
                    present[i] = false;
                    continue;
                }

                present[i] = true;

            } catch (IOException e) {
                present[i] = false;
            }
        }

        int count = 0;
        for (boolean b : present) if (b) count++;
        if (count < ErasureCoder.K) throw new IOException("lost too many shards; need " + ErasureCoder.K + ", got " + count);

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present)) {
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = reconstructed.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
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

    private static void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) {}
    }
}