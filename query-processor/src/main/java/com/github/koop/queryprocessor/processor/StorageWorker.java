package com.github.koop.queryprocessor.processor;

import com.github.koop.common.erasure.ErasureCoder;
import com.github.koop.common.messages.InputStreamMessageReader;
import com.github.koop.common.messages.MessageBuilder;
import com.github.koop.common.messages.Opcode;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.github.koop.common.erasure.ErasureCoder.TOTAL;

public final class StorageWorker {

    private static final int CONNECT_TIMEOUT_MS = 3000;

    private final List<InetSocketAddress> set1;
    private final List<InetSocketAddress> set2;
    private final List<InetSocketAddress> set3;

    private static final Logger logger = LogManager.getLogger(StorageWorker.class);

    private final ExecutorService executor;

    public StorageWorker() {
        set1 = List.of();
        set2 = List.of();
        set3 = List.of();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public StorageWorker(List<InetSocketAddress> set1, List<InetSocketAddress> set2, List<InetSocketAddress> set3) {
        if (set1.size() != TOTAL || set2.size() != TOTAL || set3.size() != TOTAL) {
            throw new IllegalArgumentException("Each set must have exactly " + TOTAL + " nodes");
        }
        this.set1 = set1;
        this.set2 = set2;
        this.set3 = set3;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public boolean put(UUID requestID, String bucket, String key, long length, InputStream data) throws IOException {

        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");
        if (data == null)
            throw new IllegalArgumentException("data is null");
        if (length < 0)
            throw new IllegalArgumentException("length < 0");

        String storageKey = bucket + "-" + key;

        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        // Shard the incoming stream. Each InputStream carries an 8-byte length prefix
        // followed by ceil(length / K*SHARD_SIZE) * SHARD_SIZE bytes of shard data.
        InputStream[] shardStreams = ErasureCoder.shard(data, length);

        long stripeDataBytes = (long) ErasureCoder.K * ErasureCoder.SHARD_SIZE;
        long numStripes = (length + stripeDataBytes - 1) / stripeDataBytes;
        long shardPayload = 8L + numStripes * ErasureCoder.SHARD_SIZE;

        // IMPORTANT: all shard streams must be drained concurrently.
        // The ErasureCodec encoder runs in one virtual thread writing to 9 pipes.
        // If we drain shards sequentially, shard[1..8]'s pipe buffers fill up while
        // we're still reading shard[0], causing the encoder to block → deadlock.
        // One virtual thread per shard fixes this.
        List<Callable<Boolean>> tasks = new LinkedList<>();

        // Connect and send STORE headers
        for (int i = 0; i < TOTAL; i++) {
            final int index = i;
            tasks.add(() -> {
                try {
                    Socket s = new Socket();
                    s.connect(nodes.get(index), CONNECT_TIMEOUT_MS);
                    s.setTcpNoDelay(true);
                    logger.trace("Connected to storage node {} for shard {}", nodes.get(index), index);
                    var out = s.getOutputStream();
                    var in = s.getInputStream();
                    MessageBuilder putMsg = new MessageBuilder(Opcode.SN_PUT);
                    putMsg.writeString(requestID.toString());
                    putMsg.writeInt(partition);
                    putMsg.writeString(storageKey);
                    putMsg.writeLargePayload(shardPayload, shardStreams[index]);
                    logger.trace("Sent PUT header for shard {} to node {}", index, nodes.get(index));
                    putMsg.writeToOutputStream(out);
                    out.flush();
                    logger.trace("Flushed PUT header for shard {} to node {}", index, nodes.get(index));
                    var reader = new InputStreamMessageReader(in);
                    var opcode = reader.getOpcode();
                    var success = reader.readByte();
                    if (opcode != Opcode.SN_PUT.getCode()) {
                        logger.trace("Unexpected opcode {} in response for shard {} from node {}", reader.getOpcode(),
                                index, nodes.get(index));
                        return false;
                    } else if (success != 1) {
                        logger.trace("PUT failed for shard {} from node {}", index, nodes.get(index));
                        return false;
                    }
                    logger.trace("Received response for shard {} from node {}", index, nodes.get(index));
                    closeQuietly(s);
                    logger.trace("Closed connection to node {} for shard {}", nodes.get(index), index);
                } catch (IOException e) {
                    logger.trace("IOException for shard {}: {}", index, e.getMessage());
                    return false;
                    // Thread.currentThread().interrupt();
                } finally {
                    logger.trace("Shard {} done, counting down latch", index);
                }
                return true;
            });
        }
        long numWritten = -1;
        try {
            logger.trace("Waiting for all shard uploads to complete");
            numWritten = executor.invokeAll(tasks).stream().map(t -> {
                try {
                    return t.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Exception in shard upload task {}", e.getMessage());
                    return false;
                }
            }).filter(it -> it).count();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        logger.trace("All shard uploads completed, wrote {} shards successfully", numWritten);
        return numWritten >= ErasureCoder.K;
    }

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;

        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);

        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 256 * 1024);

        executor.execute(() -> {
            try (pos) {
                streamReconstruct(partition, storageKey, nodes, pos);
            } catch (Exception e) {
                try {
                    pos.close();
                } catch (IOException ignored) {
                }
            }
        });

        return pis;
    }

    public boolean delete(UUID requestID, String bucket, String key) throws IOException {
        if (requestID == null)
            throw new IllegalArgumentException("requestID is null");
        if (bucket == null)
            throw new IllegalArgumentException("bucket is null");
        if (key == null)
            throw new IllegalArgumentException("key is null");

        String storageKey = bucket + "-" + key;

        int[] routing = ErasureRouting.setForKey(storageKey);
        int setNum = routing[0];
        int partition = routing[1];

        List<InetSocketAddress> nodes = nodesForKey(setNum);
        List<Callable<Boolean>> tasks = new LinkedList<>();
        for (int i = 0; i < TOTAL; i++) {
            final int index = i;
            Callable<Boolean> task = () -> {
                Socket s = new Socket();
                try {
                    s.connect(nodes.get(index), CONNECT_TIMEOUT_MS);
                    s.setTcpNoDelay(true);
                    MessageBuilder delMsg = new MessageBuilder(Opcode.SN_DELETE);
                    delMsg.writeInt(partition);
                    delMsg.writeString(storageKey);
                    var out = new BufferedOutputStream(s.getOutputStream());
                    delMsg.writeToOutputStream(out);
                    out.flush();
                    InputStreamMessageReader reader = new InputStreamMessageReader(s.getInputStream());
                    var opcode = reader.getOpcode();
                    var successByte = reader.readByte();
                    if (opcode != Opcode.SN_DELETE.getCode())
                        return false;
                    if (successByte != 1)
                        return false;
                } catch (IOException e) {
                    logger.warn("IOException in delete task for node {}: {}", nodes.get(index), e.getMessage());
                    return false;
                } finally {
                    closeQuietly(s);
                }
                return true;
            };
            tasks.add(task);
        }
        boolean success;
        try {
            success = executor.invokeAll(tasks).stream().allMatch(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    logger.warn("Exception in delete task: {}", e.getMessage());
                    return false;
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Delete operation interrupted");
            return false;
        }
        return success;
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void streamReconstruct(int partition, String storageKey, List<InetSocketAddress> nodes, OutputStream out)
            throws IOException {

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
                var opcode = reader.getOpcode();
                var successByte = reader.readByte();
                if (opcode != Opcode.SN_GET.getCode() || successByte == 0) {
                    present[i] = false;
                    continue;
                }

                present[i] = true;

            } catch (IOException e) {
                present[i] = false;
            }
        }

        int count = 0;
        for (boolean b : present)
            if (b)
                count++;
        if (count < ErasureCoder.K)
            throw new IOException("lost too many shards; need " + ErasureCoder.K + ", got " + count);

        try (InputStream reconstructed = ErasureCoder.reconstruct(ins, present)) {
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = reconstructed.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }

        out.flush();
        for (Socket s : sockets)
            if (s != null)
                closeQuietly(s);
    }

    private List<InetSocketAddress> nodesForKey(int setNum) {
        return switch (setNum) {
            case 1 -> set1;
            case 2 -> set2;
            default -> set3;
        };
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }
}