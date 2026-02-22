package com.github.koop.queryprocessor.processor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake storage node server that matches the REAL StorageNodeServer wire protocol:
 *
 * REQUEST:
 *   long frameLen   // counts bytes AFTER opcode (server passes this to handler)
 *   int opcode
 *   ... opcode fields ...
 *
 * STORE (opcode=1):
 *   requestId: string (int len + bytes)
 *   partition: int
 *   key: string
 *   payload: rest of stream (until EOF)
 * RESPONSE:
 *   long 1
 *   byte 1 on success, 0 on failure
 *
 * READ (opcode=6):
 *   partition: int
 *   key: string
 * RESPONSE:
 *   long (1 + dataLen) if found, else 1
 *   byte 1 if found else 0
 *   bytes data (if found)
 *
 * DELETE (opcode=2):
 *   partition: int
 *   key: string
 * RESPONSE:
 *   long 1
 *   byte 1 on success, 0 on failure
 */
public final class FakeStorageNodeServer implements Closeable {

    private static final int OP_STORE  = 1;
    private static final int OP_DELETE = 2;
    private static final int OP_READ   = 6;

    private final ServerSocket server;
    private final Thread acceptThread;

    private final Map<String, byte[]> store = new ConcurrentHashMap<>();
    private volatile boolean enabled = true;

    public FakeStorageNodeServer() throws IOException {
        this.server = new ServerSocket(0);
        this.acceptThread = Thread.ofVirtual().start(this::acceptLoop);
    }

    public InetSocketAddress address() {
        return new InetSocketAddress("127.0.0.1", server.getLocalPort());
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket s = server.accept();
                Thread.startVirtualThread(() -> {
                    try (s) {
                        handle(s);
                    } catch (IOException ignored) {
                        // swallow in fake server
                    }
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

        // ✅ NEW framing: read frame length first, then opcode
        long frameLen;
        int opcode;
        try {
            frameLen = in.readLong();   // not enforced; just aligns stream
            opcode = in.readInt();
        } catch (EOFException eof) {
            return;
        }

        if (!enabled) {
            // simulate node failure but keep protocol correct
            if (opcode == OP_READ) {
                out.writeLong(1L);
                out.writeByte(0);
            } else {
                out.writeLong(1L);
                out.writeByte(0);
            }
            out.flush();
            return;
        }

        switch (opcode) {
            case OP_STORE -> {
                String requestId = readString(in); // unused in fake
                int partition = in.readInt();
                String key = readString(in);

                // payload is rest of stream until EOF
                byte[] payload = readAllToEof(in);

                store.put(mapKey(partition, key), payload);

                out.writeLong(1L);
                out.writeByte(1);
                out.flush();
            }

            case OP_READ -> {
                int partition = in.readInt();
                String key = readString(in);

                byte[] data = store.get(mapKey(partition, key));
                if (data == null) {
                    out.writeLong(1L);
                    out.writeByte(0);
                    out.flush();
                } else {
                    out.writeLong(1L + data.length);
                    out.writeByte(1);
                    out.write(data);
                    out.flush();
                }
            }

            case OP_DELETE -> {
                int partition = in.readInt();
                String key = readString(in);

                store.remove(mapKey(partition, key));

                out.writeLong(1L);
                out.writeByte(1);
                out.flush();
            }

            default -> {
                out.writeLong(1L);
                out.writeByte(0);
                out.flush();
            }
        }
    }

    private static String mapKey(int partition, String key) {
        return partition + "|" + key;
    }

    private static byte[] readAllToEof(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[64 * 1024];
        while (true) {
            int r = in.read(buf);
            if (r == -1) break;
            baos.write(buf, 0, r);
        }
        return baos.toByteArray();
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
        // acceptThread will exit when server closes
    }
}
