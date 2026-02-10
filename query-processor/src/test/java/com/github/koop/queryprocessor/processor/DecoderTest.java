package com.github.koop.queryprocessor.processor;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DecoderTest {

    @Test
    void decodeRecoversOriginalBytesWithMissingShards() {
        int dataShards = 6;
        int parityShards = 3;
        int totalShards = dataShards + parityShards;

        byte[] data = new byte[50_000];
        new Random(42).nextBytes(data);

        byte[][] shards = ErasureEncoder.encode(data, dataShards, parityShards);

        boolean[] present = new boolean[totalShards];
        Arrays.fill(present, true);

        // Lose 2 shards (<= parityShards so should recover)
        shards[1] = null; present[1] = false;  // data shard
        shards[7] = null; present[7] = false;  // parity shard

        byte[] decoded = ErasureDecoder.decode(shards, present, dataShards, parityShards, data.length);

        assertArrayEquals(data, decoded);
    }

    @Test
    void decodeThrowsIfNotEnoughShardsPresent() {
        int dataShards = 4;
        int parityShards = 2;
        int totalShards = dataShards + parityShards;

        byte[] data = new byte[10_000];
        new Random(7).nextBytes(data);

        byte[][] shards = ErasureEncoder.encode(data, dataShards, parityShards);

        boolean[] present = new boolean[totalShards];
        Arrays.fill(present, true);

        // Lose 3 shards but parityShards=2 => cannot recover
        shards[0] = null; present[0] = false;
        shards[1] = null; present[1] = false;
        shards[4] = null; present[4] = false;

        assertThrows(IllegalArgumentException.class, () ->
                ErasureDecoder.decode(shards, present, dataShards, parityShards, data.length)
        );
    }
}
