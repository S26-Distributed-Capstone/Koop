package com.github.koop.queryprocessor.processor;
import com.backblaze.erasure.ReedSolomon;

import java.util.Arrays;

public final class ErasureDecoder {

    private ErasureDecoder() {}

    public static byte[] decode(
            byte[][] shards,
            boolean[] shardPresent,
            int dataShards,
            int parityShards,
            int originalLength
    ) {
        if (shards == null) throw new IllegalArgumentException("shards cannot be null");
        if (shardPresent == null) throw new IllegalArgumentException("shardPresent cannot be null");
        if (dataShards <= 0) throw new IllegalArgumentException("dataShards must be > 0");
        if (parityShards < 0) throw new IllegalArgumentException("parityShards must be >= 0");
        if (originalLength < 0) throw new IllegalArgumentException("originalLength must be >= 0");

        int totalShards = dataShards + parityShards;

        if (shards.length != totalShards) {
            throw new IllegalArgumentException("shards.length must be " + totalShards);
        }
        if (shardPresent.length != totalShards) {
            throw new IllegalArgumentException("shardPresent.length must be " + totalShards);
        }

        int presentCount = 0;
        for (boolean b : shardPresent) if (b) presentCount++;

        // If fewer than dataShards are present, reconstruction is impossible.
        if (presentCount < dataShards) {
            throw new IllegalArgumentException("Not enough shards to reconstruct data");
        }

        // Find shardSize from the first present shard
        int shardSize = -1;
        for (int i = 0; i < totalShards; i++) {
            if (shardPresent[i]) {
                if (shards[i] == null) {
                    throw new IllegalArgumentException("shardPresent[" + i + "] is true but shards[" + i + "] is null");
                }
                shardSize = shards[i].length;
                break;
            }
        }
        if (shardSize <= 0) {
            throw new IllegalArgumentException("Could not determine shardSize from present shards");
        }

        // Allocate missing shards, validate present shard sizes
        for (int i = 0; i < totalShards; i++) {
            if (shardPresent[i]) {
                if (shards[i].length != shardSize) {
                    throw new IllegalArgumentException("Shard " + i + " has wrong size");
                }
            } else {
                shards[i] = new byte[shardSize];
            }
        }

        ReedSolomon rs = ReedSolomon.create(dataShards, parityShards);
        rs.decodeMissing(shards, shardPresent, 0, shardSize);

        // Reassemble data shards
        byte[] combined = new byte[dataShards * shardSize];
        for (int i = 0; i < dataShards; i++) {
            System.arraycopy(shards[i], 0, combined, i * shardSize, shardSize);
        }

        return Arrays.copyOf(combined, originalLength);
    }
}
