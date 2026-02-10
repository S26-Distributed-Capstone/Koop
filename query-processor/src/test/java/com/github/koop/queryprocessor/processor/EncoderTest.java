package com.github.koop.queryprocessor.processor;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class EncoderTest {

    @Test
    void encodeProducesCorrectShardShapeAndNonTrivialParity() {
        int dataShards = 4;
        int parityShards = 2;
        int totalShards = dataShards + parityShards;

        byte[] data = new byte[1_000_000_000];
        new Random(1).nextBytes(data);
        long timeStart = System.currentTimeMillis();
        byte[][] shards = ErasureEncoder.encode(data, dataShards, parityShards);
        long timeEnd = System.currentTimeMillis();
        System.out.println("Time taken: " + (timeEnd - timeStart));


        // Shape checks
        assertNotNull(shards);
        assertEquals(totalShards, shards.length);

        int shardSize = shards[0].length;
        assertTrue(shardSize > 0);

        for (int i = 0; i < totalShards; i++) {
            assertNotNull(shards[i], "shard " + i + " should not be null");
            assertEquals(shardSize, shards[i].length, "all shards must be same length");
        }

        // Parity should not be all zeros (extremely unlikely with random input)
        boolean anyNonZeroInParity = false;
        for (int i = dataShards; i < totalShards; i++) {
            for (byte b : shards[i]) {
                if (b != 0) {
                    anyNonZeroInParity = true;
                    break;
                }
            }
            if (anyNonZeroInParity) break;
        }
        assertTrue(anyNonZeroInParity, "parity shards should not be all zeros");
    }

    @Test
    void encodePadsLastShardWithZerosIfNeeded() {
        int dataShards = 3;
        int parityShards = 2;

        // Length not divisible by dataShards -> padding should occur
        byte[] data = new byte[10_001];
        new Random(2).nextBytes(data);

        byte[][] shards = ErasureEncoder.encode(data, dataShards, parityShards);

        int shardSize = shards[0].length;
        int paddedLength = dataShards * shardSize;

        assertTrue(paddedLength >= data.length);

        // Bytes beyond original length should be zero in the data region
        // (in the last data shard after the original bytes end)
        int firstPaddedIndex = data.length;
        int lastPaddedIndex = paddedLength - 1;

        // Reconstruct padded data portion from the data shards and verify padded area is zeros
        byte[] reconstructedPadded = new byte[paddedLength];
        for (int i = 0; i < dataShards; i++) {
            System.arraycopy(shards[i], 0, reconstructedPadded, i * shardSize, shardSize);
        }

        for (int i = firstPaddedIndex; i <= lastPaddedIndex; i++) {
            assertEquals(0, reconstructedPadded[i], "padding byte should be zero at index " + i);
        }
    }
}
