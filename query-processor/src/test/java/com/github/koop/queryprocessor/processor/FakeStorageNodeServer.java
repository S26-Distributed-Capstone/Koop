package com.github.koop.queryprocessor.processor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock storage node matching the protocol:
 *
 * STORE (opcode 1):
 *   requestId: string
 *   partition: int
 *   key: string  (bucket-key)
 *   data: rest of stream (until EOF)
 *   -> responds 1 byte: 1 ok / 0 fail
 *
 * READ (opcode 6):
 *   partition: int
 *   key: string
 *   -> responds:
 *        long responseLen (1 + dataLen)
 *        byte successFlag (1 found / 0 not found)
 *        data bytes (if found)
 *
 * DELETE (opcode 2):
 *   partition: int
 *   key: string
 *   -> responds 1 byte: 1 ok / 0 fail
 */
final class FakeStorageNodeServer implements Closeable {

    private static final int OP_STORE = 1;
    private static final int OP_DELETE = 2;
    private static final int OP_READ = 6;

    private final ServerSocket server;
    private final Thread acceptThread;

    // key = partition|key (key already includes bucket prefix)
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();

    // Toggle to simulate node failure without killing port
    private volatile boolean enabled = true;

    FakeStorageNodeServer() throws IOException {
        this.server = new ServerSocket(0); // ephemeral port
        this.acceptThread = Thread.ofVirtual().start(this::acceptLoop);
    }

    InetSocketAddress address() {
        return new InetSocketAddress("127.0.0.1", server.getLocalPort());
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket s = server.accept();
                Thread.startVirtualThread(() -> {
                    try (s) { handle(s); }
                    catch (Exception ignored) {}
                });
            } catch (IOException e) {
                if (server.isClosed()) return;
            }
        }
    }

    private void handle(Socket s) throws IOException {
        s.setTcpNoDelay(true);

        DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));

        int opcode;
        try {
            opcode = in.readInt();
        } catch (EOFException eof) {
            return;
        }

        if (!enabled) {
            // For STORE/DELETE: send ack=0
            // For READ: send "not found" frame
            if (opcode == OP_READ) {
                out.writeLong(1L);
                out.writeByte(0);
                out.flush();
            } else {
                out.writeByte(0);
                out.flush();
            }
            return;
        }

        if (opcode == OP_STORE) {
            String requestId = readString(in); // consumed, not used
            int partition = in.readInt();
            String key = readString(in);

            // Data is "rest of stream" until client half-closes output or closes socket.
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[64 * 1024];
            while (true) {
                int r = in.read(buf);
                if (r == -1) break;
                baos.write(buf, 0, r);
            }

            store.put(k(partition, key), baos.toByteArray());

            out.writeByte(1); // ack ok
            out.flush();
            return;
        }

        if (opcode == OP_READ) {
            int partition = in.readInt();
            String key = readString(in);

            byte[] data = store.get(k(partition, key));
            if (data == null) {
                out.writeLong(1L);     // responseLen = 1 (only success byte)
                out.writeByte(0);      // not found
                out.flush();
                return;
            }

            out.writeLong(1L + data.length);
            out.writeByte(1);
            out.write(data);
            out.flush();
            return;
        }

        if (opcode == OP_DELETE) {
            int partition = in.readInt();
            String key = readString(in);
            store.remove(k(partition, key));
            out.writeByte(1);
            out.flush();
            return;
        }

        // Unknown opcode => fail
        out.writeByte(0);
        out.flush();
    }

    private static String k(int partition, String key) {
        return partition + "|" + key;
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > (1 << 20)) throw new IOException("Bad string length: " + n);
        byte[] b = in.readNBytes(n);
        if (b.length != n) throw new EOFException("Short read string");
        return new String(b, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
