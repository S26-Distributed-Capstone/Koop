package com.github.koop.common.messages;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class InputStreamMessageReader extends MessageReader {

    private final InputStream in;
    
    public InputStreamMessageReader(InputStream in) throws IOException{
        super();
        this.in = in;
        init(); // Call after initialization
    }

    @Override
    protected ByteBuffer readBytes(int length) throws IOException {
        var bytes = in.readNBytes(length);
        if (bytes.length < length) {
            throw new IOException("Unexpected end of stream");
        }
        super.remainingLength -= bytes.length;
        return ByteBuffer.wrap(bytes);
    }
}