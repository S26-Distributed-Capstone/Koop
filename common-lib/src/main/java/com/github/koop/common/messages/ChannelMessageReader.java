package com.github.koop.common.messages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ChannelMessageReader extends MessageReader {

    private final ReadableByteChannel channel;

    public ChannelMessageReader(ReadableByteChannel channel) throws IOException {
        super();
        this.channel = channel;
        init(); // Call after initialization
    }

    @Override
    protected ByteBuffer readBytes(int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        int bytesRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException("Unexpected end of channel");
            }
            bytesRead += read;
        }
        buffer.flip();
        super.remainingLength -= length; // Deduct length!
        return buffer;
    }
    
}