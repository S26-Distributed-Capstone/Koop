package com.github.koop.queryprocessor.processor;

import com.github.koop.common.messages.InputStreamMessageReader;
import com.github.koop.common.messages.MessageBuilder;
import com.github.koop.common.messages.Opcode;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake storage node server that matches the REAL StorageNodeServer wire protocol using common-lib messages
 */
public final class FakeStorageNodeServer implements Closeable {

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

        InputStream in = new BufferedInputStream(s.getInputStream());
        OutputStream out = new BufferedOutputStream(s.getOutputStream());

        InputStreamMessageReader reader;
        try {
            reader = new InputStreamMessageReader(in);
        } catch (EOFException eof) {
            return;
        }

        int opcode = reader.getOpcode();

        if (!enabled) {
            // simulate node failure but keep protocol correct
            Opcode op = getOpcodeByCode(opcode);
            if (op != null) {
                MessageBuilder err = new MessageBuilder(op);
                err.writeByte((byte) 0);
                err.writeToOutputStream(out);
                out.flush();
            }
            return;
        }

        if (opcode == Opcode.SN_PUT.getCode()) {
            String requestId = reader.readString();
            int partition = reader.readInt();
            String key = reader.readString();

            // payload is rest of stream until EOF
            byte[] payload = readAllToEof(in);

            store.put(mapKey(partition, key), payload);

            MessageBuilder resp = new MessageBuilder(Opcode.SN_PUT);
            resp.writeByte((byte) 1);
            resp.writeToOutputStream(out);
            out.flush();

        } else if (opcode == Opcode.SN_GET.getCode()) {
            int partition = reader.readInt();
            String key = reader.readString();

            byte[] data = store.get(mapKey(partition, key));
            MessageBuilder resp = new MessageBuilder(Opcode.SN_GET);
            if (data == null) {
                resp.writeByte((byte) 0);
            } else {
                resp.writeByte((byte) 1);
                resp.writeLargePayload(data.length, new ByteArrayInputStream(data));
            }
            resp.writeToOutputStream(out);
            out.flush();

        } else if (opcode == Opcode.SN_DELETE.getCode()) {
            int partition = reader.readInt();
            String key = reader.readString();

            store.remove(mapKey(partition, key));

            MessageBuilder resp = new MessageBuilder(Opcode.SN_DELETE);
            resp.writeByte((byte) 1);
            resp.writeToOutputStream(out);
            out.flush();

        } else {
            // Unrecognized opcode
            MessageBuilder resp = new MessageBuilder(Opcode.SN_GET);
            resp.writeByte((byte) 0);
            resp.writeToOutputStream(out);
            out.flush();
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

    private Opcode getOpcodeByCode(int code) {
        for (Opcode op : Opcode.values()) {
            if (op.getCode() == code) return op;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        server.close();
        // acceptThread will exit when server closes
    }
}