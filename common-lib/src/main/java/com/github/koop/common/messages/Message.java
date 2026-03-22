package com.github.koop.common.messages;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public sealed interface Message permits Message.FileCommitMessage, Message.MultipartCommitMessage, Message.DeleteMessage,Message.CreateBucketMessage,Message.DeleteBucketMessage {
    InetSocketAddress sender();
    String requestID();
    byte[] serialize();

    // Helper methods for direct memory access
    static byte[] getBytes(String str) {
        return str != null ? str.getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    static void putString(ByteBuffer buffer, String str) {
        byte[] bytes = getBytes(str);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    static String getString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public record FileCommitMessage(String bucket, String key, String requestID, InetSocketAddress sender) implements Message {
        @Override
        public byte[] serialize() {
            byte[] bucketBytes = getBytes(bucket);
            byte[] keyBytes = getBytes(key);
            byte[] reqBytes = getBytes(requestID);
            byte[] hostBytes = getBytes(sender.getHostName());
            
            int capacity = 4 + bucketBytes.length + 
                           4 + keyBytes.length + 
                           4 + reqBytes.length + 
                           4 + hostBytes.length + 
                           4; // port

            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.putInt(bucketBytes.length).put(bucketBytes);
            buffer.putInt(keyBytes.length).put(keyBytes);
            buffer.putInt(reqBytes.length).put(reqBytes);
            buffer.putInt(hostBytes.length).put(hostBytes);
            buffer.putInt(sender.getPort());

            return buffer.array();
        }

        public static FileCommitMessage deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String bucket = getString(buffer);
            String key = getString(buffer);
            String requestID = getString(buffer);
            String host = getString(buffer);
            int port = buffer.getInt();
            return new FileCommitMessage(bucket, key, requestID, new InetSocketAddress(host, port));
        }
    }

    public record MultipartCommitMessage(String bucket, String key, String requestID, InetSocketAddress sender, List<String> chunks) implements Message {
        @Override
        public byte[] serialize() {
            byte[] bucketBytes = getBytes(bucket);
            byte[] keyBytes = getBytes(key);
            byte[] reqBytes = getBytes(requestID);
            byte[] hostBytes = getBytes(sender.getHostName());

            int capacity = 4 + bucketBytes.length + 
                           4 + keyBytes.length + 
                           4 + reqBytes.length + 
                           4 + hostBytes.length + 
                           4 + // port
                           4;  // chunk list size
                           
            List<byte[]> chunkBytesList = new ArrayList<>(chunks.size());
            for (String chunk : chunks) {
                byte[] chunkBytes = getBytes(chunk);
                chunkBytesList.add(chunkBytes);
                capacity += 4 + chunkBytes.length;
            }

            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.putInt(bucketBytes.length).put(bucketBytes);
            buffer.putInt(keyBytes.length).put(keyBytes);
            buffer.putInt(reqBytes.length).put(reqBytes);
            buffer.putInt(hostBytes.length).put(hostBytes);
            buffer.putInt(sender.getPort());
            
            buffer.putInt(chunks.size());
            for (byte[] chunkBytes : chunkBytesList) {
                buffer.putInt(chunkBytes.length).put(chunkBytes);
            }

            return buffer.array();
        }

        public static MultipartCommitMessage deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String bucket = getString(buffer);
            String key = getString(buffer);
            String requestID = getString(buffer);
            String host = getString(buffer);
            int port = buffer.getInt();
            
            int size = buffer.getInt();
            List<String> chunks = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                chunks.add(getString(buffer));
            }
            return new MultipartCommitMessage(bucket, key, requestID, new InetSocketAddress(host, port), chunks);
        }
    }

    public record DeleteMessage(String bucket, String key, String requestID, InetSocketAddress sender) implements Message {
        @Override
        public byte[] serialize() {
            byte[] bucketBytes = getBytes(bucket);
            byte[] keyBytes = getBytes(key);
            byte[] reqBytes = getBytes(requestID);
            byte[] hostBytes = getBytes(sender.getHostName());
            
            int capacity = 4 + bucketBytes.length + 
                           4 + keyBytes.length + 
                           4 + reqBytes.length + 
                           4 + hostBytes.length + 
                           4; // port

            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.putInt(bucketBytes.length).put(bucketBytes);
            buffer.putInt(keyBytes.length).put(keyBytes);
            buffer.putInt(reqBytes.length).put(reqBytes);
            buffer.putInt(hostBytes.length).put(hostBytes);
            buffer.putInt(sender.getPort());

            return buffer.array();
        }

        public static DeleteMessage deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String bucket = getString(buffer);
            String key = getString(buffer);
            String requestID = getString(buffer);
            String host = getString(buffer);
            int port = buffer.getInt();
            return new DeleteMessage(bucket, key, requestID, new InetSocketAddress(host, port));
        }
    }

    public record CreateBucketMessage(String bucket, String requestID, InetSocketAddress sender) implements Message {
        @Override
        public byte[] serialize() {
            byte[] bucketBytes = getBytes(bucket);
            byte[] reqBytes = getBytes(requestID);
            byte[] hostBytes = getBytes(sender.getHostName());
            
            int capacity = 4 + bucketBytes.length + 
                           4 + reqBytes.length + 
                           4 + hostBytes.length + 
                           4; // port

            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.putInt(bucketBytes.length).put(bucketBytes);
            buffer.putInt(reqBytes.length).put(reqBytes);
            buffer.putInt(hostBytes.length).put(hostBytes);
            buffer.putInt(sender.getPort());

            return buffer.array();
        }

        public static CreateBucketMessage deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String bucket = getString(buffer);
            String requestID = getString(buffer);
            String host = getString(buffer);
            int port = buffer.getInt();
            return new CreateBucketMessage(bucket, requestID, new InetSocketAddress(host, port));
        }
    }

    public record DeleteBucketMessage(String bucket, String requestID, InetSocketAddress sender) implements Message {
        @Override
        public byte[] serialize() {
            byte[] bucketBytes = getBytes(bucket);
            byte[] reqBytes = getBytes(requestID);
            byte[] hostBytes = getBytes(sender.getHostName());
            
            int capacity = 4 + bucketBytes.length + 
                           4 + reqBytes.length + 
                           4 + hostBytes.length + 
                           4; // port

            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.putInt(bucketBytes.length).put(bucketBytes);
            buffer.putInt(reqBytes.length).put(reqBytes);
            buffer.putInt(hostBytes.length).put(hostBytes);
            buffer.putInt(sender.getPort());

            return buffer.array();
        }

        public static DeleteBucketMessage deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String bucket = getString(buffer);
            String requestID = getString(buffer);
            String host = getString(buffer);
            int port = buffer.getInt();
            return new DeleteBucketMessage(bucket, requestID, new InetSocketAddress(host, port));
        }
    }
}


