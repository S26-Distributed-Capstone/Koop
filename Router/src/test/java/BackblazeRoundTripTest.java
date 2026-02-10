import com.backblaze.erasure.ReedSolomon;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class BackblazeRoundTripTest {

    @Test
    void roundTripRecoversMissingShards() {
        int dataShards = 6;
        int parityShards = 3;
        int totalShards = dataShards + parityShards;

        byte[] original = new byte[5_000_000];
        new Random(123).nextBytes(original);

        int shardSize = (original.length + dataShards - 1) / dataShards;

        byte[][] shards = new byte[totalShards][shardSize];

        // Fill data shards (pad zeros automatically)
        int offset = 0;
        for (int i = 0; i < dataShards; i++) {
            int bytesToCopy = Math.min(shardSize, original.length - offset);
            if (bytesToCopy > 0) {
                System.arraycopy(original, offset, shards[i], 0, bytesToCopy);
                offset += bytesToCopy;
            }
        }

        ReedSolomon rs = ReedSolomon.create(dataShards, parityShards);
        rs.encodeParity(shards, 0, shardSize);

        // Simulate losing 2 shards (<= parityShards so recovery should succeed)
        boolean[] present = new boolean[totalShards];
        Arrays.fill(present, true);

        shards[1] = new byte[shardSize];  // lost data shard
        present[1] = false;

        shards[7] = new byte[shardSize];  // lost parity shard
        present[7] = false;

        rs.decodeMissing(shards, present, 0, shardSize);

        // Reassemble recovered data from data shards
        byte[] recoveredPadded = new byte[dataShards * shardSize];
        for (int i = 0; i < dataShards; i++) {
            System.arraycopy(shards[i], 0, recoveredPadded, i * shardSize, shardSize);
        }

        byte[] recovered = Arrays.copyOf(recoveredPadded, original.length);
        assertArrayEquals(original, recovered);
    }
}
