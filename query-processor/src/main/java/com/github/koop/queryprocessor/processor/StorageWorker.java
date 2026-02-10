package com.github.koop.queryprocessor.processor;
import com.backblaze.erasure.ReedSolomon;
import jdk.jshell.spi.ExecutionControl;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Worker implements:
 *
 * PUT (gateway opcode 1):
 *   gateway sends: int opcode, requestId(str), bucket(str), key(str), long originalLength, then originalLength bytes
 *   worker sends to each node a shard stream:
 *      [8 bytes originalLength][block0][block1]...[blockN-1]
 *   node header includes shardLength = 8 + N*SHARD_SIZE
 *
 * GET (gateway opcode 6):
 *   gateway sends: int opcode, requestId(str), bucket(str), key(str)
 *   worker requests shards from nodes, reads shardLength from node header,
 *   then reads first 8 bytes of shard stream (originalLength), verifies,
 *   then reads N fixed blocks per shard, decodes and streams originalLength bytes back to gateway.
 *
 * Node protocol assumed:
 *
 * PUT shard (worker->node):
 *   int opcode=1
 *   string bucket
 *   string key
 *   int partition
 *   long shardLengthBytes  (includes the first 8 bytes of originalLength)
 *   then shardLengthBytes bytes:
 *     [8 bytes originalLength][N blocks of SHARD_SIZE]
 * node->worker ack:
 *   int status (0 ok)
 *   string msg
 *
 * GET shard (worker->node):
 *   int opcode=6
 *   string bucket
 *   string key
 *   int partition
 * node->worker response:
 *   int status (0 ok)
 *   string msg
 *   long shardLengthBytes
 *   then shardLengthBytes bytes stream:
 *     [8 bytes originalLength][N blocks of SHARD_SIZE]
 */
public final class StorageWorker{

    // Gateway opcodes
    private static final int OP_PUT = 1;
    private static final int OP_GET = 6;

    // Node opcodes
    private static final int NODE_PUT_SHARD = 1;
    private static final int NODE_GET_SHARD = 6;

    // Erasure coding: tolerate 3 failures
    private static final int K = 6;
    private static final int M = 3;
    private static final int TOTAL = K + M;

    // Stripe block size
    private static final int SHARD_SIZE = 1 << 20; // 1MB

    // Node connect timeout (ms)
    private static final int NODE_CONNECT_TIMEOUT_MS = 3000;

    private final List<InetSocketAddress> set1, set2, set3;

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

    public void serve(int port) throws IOException {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("StorageWorkerFixedHeader listening on " + port);
            while (true) {
                Socket gw = server.accept();
                Thread.startVirtualThread(() -> {
                    try (gw) {
                        handleGateway(gw);
                    } catch (Exception e) {
                        System.err.println("Request failed: " + e);
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public boolean put(UUID requestID, String bucket, String key, long length, InputStream data) throws IOException {
        /*
        finds set
        encodes,
        read in TOTAL bytes
        write to output streams
        return true on success, false on fail
         */

        throw new UnsupportedOperationException("not implemented yet :)");
    }

    public InputStream get(UUID requestID, String bucket, String key) throws IOException {
        /*
        finds set
        reconstructs data from set
        streams out
         */
        throw new UnsupportedOperationException("not implemented yet :)");
    }

    public boolean delete(UUID requestID, String bucket, String key) throws IOException {
        /*
        sends to delete
        returns true on success false on fail
         */
        throw new UnsupportedOperationException("not implemented yet :)");
    }

    // ------------------ Gateway protocol helpers (no Wire) ------------------

    private static void writeUtf8String(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readUtf8String(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > (1 << 20)) throw new IOException("Bad string length: " + n);
        byte[] b = new byte[n];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    // ------------------ Gateway request handler ------------------

    private void handleGateway(Socket gw) throws IOException {
        gw.setTcpNoDelay(true);

        DataInputStream in = new DataInputStream(new BufferedInputStream(gw.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(gw.getOutputStream()));

        int opcode = in.readInt();
        readUtf8String(in); // requestId ignored (but consumed)

        String bucket = readUtf8String(in);
        String key = readUtf8String(in);

        try {
            if (opcode == OP_PUT) {
                long originalLength = in.readLong();
                putObject(bucket, key, originalLength, in);
                writeOk(out, "stored");
            } else if (opcode == OP_GET) {
                getObject(bucket, key, out);
            } else {
                writeErr(out, 400, "unsupported opcode " + opcode);
            }
        } catch (Exception e) {
            writeErr(out, 500, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    // ------------------ PUT ------------------

    private void putObject(String bucket, String key, long originalLength, InputStream gatewayBody) throws Exception {
        if (originalLength < 0) throw new IllegalArgumentException("originalLength < 0");

        List<InetSocketAddress> nodes = nodesForKey(key);

        long stripeDataBytes = (long) K * SHARD_SIZE;
        long numStripes = (originalLength + stripeDataBytes - 1) / stripeDataBytes;

        // shard stream length includes 8-byte originalLength header at the start
        long shardLength = 8L + numStripes * SHARD_SIZE;

        Socket[] sockets = new Socket[TOTAL];
        DataOutputStream[] nodeOut = new DataOutputStream[TOTAL];
        DataInputStream[] nodeIn = new DataInputStream[TOTAL];

        try {
            // Connect and send node PUT headers
            for (int p = 0; p < TOTAL; p++) {
                Socket s = new Socket();
                s.connect(nodes.get(p), NODE_CONNECT_TIMEOUT_MS);
                s.setTcpNoDelay(true);
                sockets[p] = s;

                nodeOut[p] = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                nodeIn[p]  = new DataInputStream(new BufferedInputStream(s.getInputStream()));

                nodeOut[p].writeInt(NODE_PUT_SHARD);
                writeUtf8String(nodeOut[p], bucket);
                writeUtf8String(nodeOut[p], key);
                nodeOut[p].writeInt(p);
                nodeOut[p].writeLong(shardLength);
                nodeOut[p].flush();
            }

            // Write the 8-byte originalLength header into EVERY shard stream
            for (int p = 0; p < TOTAL; p++) {
                nodeOut[p].writeLong(originalLength);
            }

            ReedSolomon rs = ReedSolomon.create(K, M);

            byte[] stripeBuf = new byte[K * SHARD_SIZE];
            byte[][] shards = new byte[TOTAL][SHARD_SIZE];

            long remaining = originalLength;

            while (remaining > 0) {
                int want = (int) Math.min((long) stripeBuf.length, remaining);
                readFully(gatewayBody, stripeBuf, 0, want);
                remaining -= want;

                // pad last stripe with zeros
                if (want < stripeBuf.length) Arrays.fill(stripeBuf, want, stripeBuf.length, (byte) 0);

                // Fill data shards 0..K-1
                for (int i = 0; i < K; i++) {
                    System.arraycopy(stripeBuf, i * SHARD_SIZE, shards[i], 0, SHARD_SIZE);
                }
                // Clear parity shards K..TOTAL-1
                for (int i = K; i < TOTAL; i++) Arrays.fill(shards[i], (byte) 0);

                // Compute parity
                rs.encodeParity(shards, 0, SHARD_SIZE);

                // Write one shard block to each node
                for (int p = 0; p < TOTAL; p++) {
                    nodeOut[p].write(shards[p], 0, SHARD_SIZE);
                }
            }

            for (int p = 0; p < TOTAL; p++) nodeOut[p].flush();

            // Read node ACKs
            for (int p = 0; p < TOTAL; p++) {
                int st = nodeIn[p].readInt();
                String msg = readUtf8String(nodeIn[p]);
                if (st != 0) throw new IOException("node " + p + " PUT failed: " + msg);
            }

        } finally {
            for (Socket s : sockets) if (s != null) try { s.close(); } catch (IOException ignored) {}
        }
    }

    // ------------------ GET ------------------

    private void getObject(String bucket, String key, DataOutputStream gatewayOut) throws Exception {
        List<InetSocketAddress> nodes = nodesForKey(key);

        Socket[] sockets = new Socket[TOTAL];
        DataInputStream[] nodeIn = new DataInputStream[TOTAL];
        boolean[] present = new boolean[TOTAL];

        long shardLength = -1;

        try {
            // Request each shard
            for (int p = 0; p < TOTAL; p++) {
                try {
                    Socket s = new Socket();
                    s.connect(nodes.get(p), NODE_CONNECT_TIMEOUT_MS);
                    s.setTcpNoDelay(true);
                    sockets[p] = s;

                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                    nodeIn[p] = new DataInputStream(new BufferedInputStream(s.getInputStream()));

                    out.writeInt(NODE_GET_SHARD);
                    writeUtf8String(out, bucket);
                    writeUtf8String(out, key);
                    out.writeInt(p);
                    out.flush();

                    int st = nodeIn[p].readInt();
                    String msg = readUtf8String(nodeIn[p]);
                    if (st != 0) {
                        present[p] = false;
                        s.close(); sockets[p] = null; nodeIn[p] = null;
                    } else {
                        long sl = nodeIn[p].readLong(); // shardLength includes 8-byte header
                        if (sl < 8) {
                            present[p] = false;
                            s.close(); sockets[p] = null; nodeIn[p] = null;
                        } else {
                            present[p] = true;
                            if (shardLength < 0) shardLength = sl;
                            else if (shardLength != sl) {
                                // inconsistent sizes -> treat as corrupt shard
                                present[p] = false;
                                s.close(); sockets[p] = null; nodeIn[p] = null;
                            }
                        }
                    }
                } catch (IOException e) {
                    present[p] = false;
                    if (sockets[p] != null) try { sockets[p].close(); } catch (IOException ignored) {}
                    sockets[p] = null;
                    nodeIn[p] = null;
                }
            }

            int have = 0;
            for (boolean b : present) if (b) have++;
            if (have < K) {
                writeErr(gatewayOut, 404, "not enough shards (" + have + "/" + K + ")");
                return;
            }
            if (shardLength < 0) {
                writeErr(gatewayOut, 500, "no shardLength from any node");
                return;
            }

            // Determine number of stripes from shardLength
            long blockBytes = shardLength - 8L;
            if (blockBytes < 0 || (blockBytes % SHARD_SIZE) != 0) {
                writeErr(gatewayOut, 500, "bad shardLength " + shardLength);
                return;
            }
            long numStripes = blockBytes / SHARD_SIZE;

            // Read the first 8 bytes (originalLength) from each shard stream and verify they match
            long originalLength = -1;
            for (int p = 0; p < TOTAL; p++) {
                if (!present[p] || nodeIn[p] == null) continue;

                try {
                    long L = nodeIn[p].readLong();
                    if (originalLength < 0) originalLength = L;
                    else if (originalLength != L) {
                        // mismatch => shard is corrupt
                        present[p] = false;
                        closeQuietly(sockets[p]);
                        sockets[p] = null;
                        nodeIn[p] = null;
                    }
                } catch (IOException e) {
                    present[p] = false;
                    closeQuietly(sockets[p]);
                    sockets[p] = null;
                    nodeIn[p] = null;
                }
            }

            int haveAfterHeader = 0;
            for (boolean b : present) if (b) haveAfterHeader++;
            if (haveAfterHeader < K) {
                writeErr(gatewayOut, 404, "not enough valid shards after header (" + haveAfterHeader + "/" + K + ")");
                return;
            }
            if (originalLength < 0) {
                writeErr(gatewayOut, 500, "could not read originalLength from shards");
                return;
            }

            // Respond to gateway with the reconstructed object length
            gatewayOut.writeInt(0);
            writeUtf8String(gatewayOut, "ok");
            gatewayOut.writeLong(originalLength);
            gatewayOut.flush();

            ReedSolomon rs = ReedSolomon.create(K, M);

            byte[][] shards = new byte[TOTAL][SHARD_SIZE];
            boolean[] stripePresent = Arrays.copyOf(present, present.length);

            long remaining = originalLength;

            // Decode each stripe and stream bytes back
            for (long stripe = 0; stripe < numStripes && remaining > 0; stripe++) {
                int presentCount = 0;

                for (int p = 0; p < TOTAL; p++) {
                    if (!stripePresent[p] || nodeIn[p] == null) {
                        Arrays.fill(shards[p], (byte) 0);
                        stripePresent[p] = false;
                        continue;
                    }

                    boolean ok = readExactly(nodeIn[p], shards[p], 0, SHARD_SIZE);
                    if (ok) {
                        presentCount++;
                    } else {
                        // Node died mid-stream; mark shard missing from now on
                        stripePresent[p] = false;
                        nodeIn[p] = null;
                        Arrays.fill(shards[p], (byte) 0);
                    }
                }

                if (presentCount < K) {
                    throw new IOException("cannot reconstruct stripe " + stripe + " (have " + presentCount + ", need " + K + ")");
                }

                // Reconstruct missing shard blocks
                rs.decodeMissing(shards, stripePresent, 0, SHARD_SIZE);

                // The original data bytes for this stripe are in data shards 0..K-1
                for (int i = 0; i < K && remaining > 0; i++) {
                    int toWrite = (int) Math.min((long) SHARD_SIZE, remaining);
                    gatewayOut.write(shards[i], 0, toWrite);
                    remaining -= toWrite;
                }
                gatewayOut.flush();
            }

            if (remaining > 0) {
                throw new IOException("ran out of shard data before completing object; remaining=" + remaining);
            }

        } finally {
            for (Socket s : sockets) if (s != null) closeQuietly(s);
        }
    }

    // ------------------ Routing ------------------

    private List<InetSocketAddress> nodesForKey(String key) {
        return switch (ErasureRouting.setForKey(key)) {
            case 1 -> set1;
            case 2 -> set2;
            case 3 -> set3;
            default -> throw new IllegalStateException("bad set");
        };
    }

    // ------------------ Worker->Gateway response helpers ------------------

    private static void writeOk(DataOutputStream out, String msg) throws IOException {
        out.writeInt(0);
        writeUtf8String(out, msg);
        out.flush();
    }

    private static void writeErr(DataOutputStream out, int code, String msg) throws IOException {
        out.writeInt(code);
        writeUtf8String(out, msg);
        out.flush();
    }

    // ------------------ IO helpers ------------------

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
