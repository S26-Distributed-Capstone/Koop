package com.github.koop.common.messages;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class MessageReader {
    protected long remainingLength;
    private final int opcode;
    
    public MessageReader() throws IOException{
        this.remainingLength = readLong();
        this.opcode = readInt();
    }

    protected abstract ByteBuffer readBytes(int length) throws IOException;

    public int readInt() throws IOException {
        return readBytes(4).getInt();
    }

    public long readLong() throws IOException {
        return readBytes(8).getLong();
    }

    public short readShort() throws IOException {
        return readBytes(2).getShort();
    }

    public byte readByte() throws IOException {
        return readBytes(1).get();
    }

    public String readString() throws IOException {
        int length = readInt(); // Read the length of the string first
        var byteBuf = readBytes(length);
        return new String(byteBuf.array(), byteBuf.position(), byteBuf.remaining());
    }

    public long getRemainingLength() {
        return remainingLength;
    }
}
