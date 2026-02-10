import org.junit.jupiter.api.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageWorkerFixedHeaderTest {

    private static final int OP_PUT = 1;
    private static final int OP_GET = 6;
    private static final int WORKER_PORT = 19000;

    private List<FakeStorageNodeServer> nodes;

    @BeforeAll
    void startCluster() throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 9; i++) nodes.add(new FakeStorageNodeServer());

        List<java.net.InetSocketAddress> set = nodes.stream().map(FakeStorageNodeServer::address).toList();

        StorageWorker worker = new StorageWorker(set, set, set);

        Thread.ofVirtual().start(() -> {
            try {
                worker.serve(WORKER_PORT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Thread.sleep(100); // let server bind
    }

    @BeforeEach
    void resetNodesUp() {
        for (FakeStorageNodeServer n : nodes) n.setEnabled(true);
    }

    @AfterAll
    void stopNodes() throws Exception {
        for (FakeStorageNodeServer n : nodes) n.close();
    }

    @Test
    void putThenGet_roundTrip() throws Exception {
        String bucket = "b";
        String key = "fileA";

        byte[] data = new byte[400_000_000];
        new SecureRandom().nextBytes(data);

        put(bucket, key, data);
        byte[] got = get(bucket, key);

        assertArrayEquals(data, got);
    }

    @Test
    void get_withThreeNodeFailures_stillWorks() throws Exception {
        String bucket = "b";
        String key = "fileB";

        byte[] data = new byte[400_000_000];
        new SecureRandom().nextBytes(data);

        put(bucket, key, data);

        // simulate 3 nodes down AFTER PUT (M=3 tolerated)
        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);

        byte[] got = get(bucket, key);
        assertArrayEquals(data, got);
    }

    @Test
    void get_withFourNodeFailures_fails() throws Exception {
        String bucket = "b";
        String key = "fileC";

        byte[] data = new byte[400_000_000];
        new SecureRandom().nextBytes(data);

        put(bucket, key, data);

        // simulate 4 nodes down AFTER PUT (NOT tolerated)
        nodes.get(0).setEnabled(false);
        nodes.get(1).setEnabled(false);
        nodes.get(2).setEnabled(false);
        nodes.get(3).setEnabled(false);

        IOException ex = assertThrows(IOException.class, () -> get(bucket, key));
        assertNotNull(ex.getMessage());
    }

    // ---------------- gateway client helpers ----------------

    private void put(String bucket, String key, byte[] data) throws Exception {
        try (Socket s = new Socket("127.0.0.1", WORKER_PORT)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));

            out.writeInt(OP_PUT);
            writeUtf8String(out, "req-put");
            writeUtf8String(out, bucket);
            writeUtf8String(out, key);
            out.writeLong(data.length);
            out.write(data);
            out.flush();

            int st = in.readInt();
            String msg = readUtf8String(in);
            if (st != 0) throw new IOException("PUT failed: " + st + " " + msg);
        }
    }

    private byte[] get(String bucket, String key) throws Exception {
        try (Socket s = new Socket("127.0.0.1", WORKER_PORT)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));

            out.writeInt(OP_GET);
            writeUtf8String(out, "req-get");
            writeUtf8String(out, bucket);
            writeUtf8String(out, key);
            out.flush();

            int st = in.readInt();
            String msg = readUtf8String(in);
            if (st != 0) throw new IOException("GET failed: " + st + " " + msg);

            long len = in.readLong();
            if (len < 0 || len > (1L << 31)) throw new IOException("bad length " + len);

            byte[] data = in.readNBytes((int) len);
            if (data.length != (int) len) throw new IOException("short read from worker");
            return data;
        }
    }

    private static void writeUtf8String(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readUtf8String(DataInputStream in) throws IOException {
        int n = in.readInt();
        byte[] b = new byte[n];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
