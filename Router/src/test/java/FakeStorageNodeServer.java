import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class FakeStorageNodeServer implements Closeable {

    private static final int NODE_PUT_SHARD = 1;
    private static final int NODE_GET_SHARD = 6;

    private final ServerSocket server;
    private final Thread acceptThread;

    // key: bucket|key|partition  value: shard bytes
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();

    // Flip this to simulate a node going “down” without freeing the port
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

    boolean isEnabled() {
        return enabled;
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
        DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));

        // If “down”, immediately respond error and close connection cleanly.
        if (!enabled) {
            int opcode;
            try {
                opcode = in.readInt(); // consume
            } catch (IOException e) {
                return;
            }
            out.writeInt(503);
            writeUtf8String(out, "node down");
            // for GET, worker expects a shardLength only if status==0, so we stop here
            out.flush();
            return;
        }

        int opcode = in.readInt();

        if (opcode == NODE_PUT_SHARD) {
            String bucket = readUtf8String(in);
            String key = readUtf8String(in);
            int partition = in.readInt();
            long shardLength = in.readLong();

            if (shardLength < 0 || shardLength > (1L << 31)) {
                out.writeInt(400);
                writeUtf8String(out, "bad shardLength");
                out.flush();
                return;
            }

            byte[] data = in.readNBytes((int) shardLength);
            if (data.length != (int) shardLength) {
                out.writeInt(500);
                writeUtf8String(out, "short read");
                out.flush();
                return;
            }

            store.put(k(bucket, key, partition), data);

            out.writeInt(0);
            writeUtf8String(out, "ok");
            out.flush();
            return;
        }

        if (opcode == NODE_GET_SHARD) {
            String bucket = readUtf8String(in);
            String key = readUtf8String(in);
            int partition = in.readInt();

            byte[] data = store.get(k(bucket, key, partition));
            if (data == null) {
                out.writeInt(404);
                writeUtf8String(out, "missing");
                out.flush();
                return;
            }

            out.writeInt(0);
            writeUtf8String(out, "ok");
            out.writeLong(data.length); // shardLength includes the first 8 bytes
            out.write(data);
            out.flush();
            return;
        }

        out.writeInt(400);
        writeUtf8String(out, "bad opcode");
        out.flush();
    }

    private static String k(String bucket, String key, int partition) {
        return bucket + "|" + key + "|" + partition;
    }

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

    @Override
    public void close() throws IOException {
        server.close();
    }
}
