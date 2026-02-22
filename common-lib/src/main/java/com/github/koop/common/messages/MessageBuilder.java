package com.github.koop.common.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class MessageBuilder {
    private long length;
    private final ByteBuffer buffer;
    private InputStream largePayloadDataInputStream;
    private ReadableByteChannel largePayloadDataChannel;
    private long payloadLength;
    
    public MessageBuilder(Opcode opcode) {
        this.buffer = ByteBuffer.allocate(1024); // Initial size, can be adjusted
        this.buffer.putInt(opcode.getCode()); // Write opcode first
        this.length+=4; // Account for the opcode length
    }

    public void writeInt(int value) {
        buffer.putInt(value);
        length += 4;
    }

    public void writeLong(long value) {
        buffer.putLong(value);
        length += 8;
    }

    public void writeShort(short value) {
        buffer.putShort(value);
        length += 2;
    }

    public void writeByte(byte value) {
        buffer.put(value);
        length += 1;
    }

    public void writeString(String value) {
        byte[] strBytes = value.getBytes();
        buffer.putInt(strBytes.length); // Write length of the string first
        buffer.put(strBytes); // Write the string bytes
        length += 4 + strBytes.length; // Account for length and string bytes
    }
    

    public void writeLargePayload(long length, InputStream data){
        this.length+=length;
        this.largePayloadDataInputStream = data;
        this.payloadLength = length;
    }

    public void writeLargePayload(long length, ReadableByteChannel channel){
        this.length += length;
        this.largePayloadDataChannel = channel;
        this.payloadLength = length;
    }


    public void writeToOutputStream(OutputStream out) throws IOException {
        // First, write the total length (excluding the length field itself)
        ByteBuffer lengthBuffer = ByteBuffer.allocate(8);
        lengthBuffer.putLong(length);
        out.write(lengthBuffer.array());

        // Then, write the buffered data
        buffer.flip(); // Prepare buffer for reading
        out.write(buffer.array(), 0, buffer.limit());

        // Finally, if there's a large payload, write it directly from the InputStream
        if (largePayloadDataInputStream != null) {
            byte[] chunk = new byte[8192];
            long remaining = payloadLength;
            int bytesRead;
            while (remaining > 0 && (bytesRead = largePayloadDataInputStream.read(chunk, 0, (int) Math.min(chunk.length, remaining))) != -1) {
                out.write(chunk, 0, bytesRead);
                remaining -= bytesRead;
            }
        } else if (largePayloadDataChannel != null) {
            if (largePayloadDataChannel instanceof FileChannel) {
                transferAll((FileChannel) largePayloadDataChannel, java.nio.channels.Channels.newChannel(out), payloadLength);
            } else {
                ReadableByteChannel src = largePayloadDataChannel;
                WritableByteChannel dest = java.nio.channels.Channels.newChannel(out);
                ByteBuffer copyBuffer = ByteBuffer.allocate(8192);
                long remaining = payloadLength;
                while (remaining > 0) {
                    copyBuffer.clear();
                    if (remaining < copyBuffer.capacity()) {
                        copyBuffer.limit((int) remaining);
                    }
                    int n = src.read(copyBuffer);
                    if (n == -1) {
                        break;
                    }
                    if (n == 0) {
                        continue;
                    }
                    copyBuffer.flip();
                    while (copyBuffer.hasRemaining()) {
                        dest.write(copyBuffer);
                    }
                    remaining -= n;
                }
            }
        }
    }

    private long transferAll(FileChannel src, WritableByteChannel dest, long count) throws IOException {
        long transferred = 0;
        long initialPosition = src.position();
        while (transferred < count) {
            long n = src.transferTo(initialPosition + transferred, count - transferred, dest);
            if (n <= 0) {
                break;
            }
            transferred += n;
        }
        return transferred;
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException {
        // First, write the total length (excluding the length field itself)
        ByteBuffer lengthBuffer = ByteBuffer.allocate(8);
        lengthBuffer.putLong(length);
        lengthBuffer.flip();
        channel.write(lengthBuffer);

        // Then, write the buffered data
        buffer.flip(); // Prepare buffer for reading
        channel.write(buffer);

        // Finally, if there's a large payload, write it directly from the InputStream
        if (largePayloadDataInputStream != null) {
            byte[] chunk = new byte[8192]; // 8KB buffer for streaming
            long remaining = payloadLength;
            int bytesRead;
            while (remaining > 0 && (bytesRead = largePayloadDataInputStream.read(chunk, 0, (int) Math.min(chunk.length, remaining))) != -1) {
                ByteBuffer chunkBuffer = ByteBuffer.wrap(chunk, 0, bytesRead);
                while (chunkBuffer.hasRemaining()) {
                    channel.write(chunkBuffer);
                }
                remaining -= bytesRead;
            }
        } else if (largePayloadDataChannel != null) {
            if (largePayloadDataChannel instanceof FileChannel) {
                transferAll((FileChannel) largePayloadDataChannel, channel, payloadLength);
            } else {
                ReadableByteChannel src = largePayloadDataChannel;
                ByteBuffer copyBuffer = ByteBuffer.allocate(8192);
                long remaining = payloadLength;
                while (remaining > 0) {
                    // Limit buffer to read only what we need
                    copyBuffer.clear();
                    if (remaining < copyBuffer.capacity()) {
                        copyBuffer.limit((int) remaining);
                    }
                    
                    int n = src.read(copyBuffer);
                    if (n == -1) {
                        break;
                    }
                    if (n == 0) {
                        continue; // or handle blocking? But here we assume it will eventually read or end.
                    }

                    copyBuffer.flip();
                    while (copyBuffer.hasRemaining()) {
                        channel.write(copyBuffer);
                    }
                    remaining -= n;
                }
            }
        }
    }




}
