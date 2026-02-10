import com.backblaze.erasure.ReedSolomon;

public final class ErasureEncoder {

    private ErasureEncoder() {}

    public static byte[][] encode(byte[] data, int dataShards, int parityShards) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        if (dataShards <= 0) {
            throw new IllegalArgumentException("dataShards must be > 0");
        }
        if (parityShards <= 0) {
            throw new IllegalArgumentException("parityShards must be > 0");
        }

        int totalShards = dataShards + parityShards;

        // shardSize = ceil(data.length / dataShards); allow empty data (length 0)
        int shardSize = (data.length + dataShards - 1) / dataShards;

        // Backblaze encodeParity expects shardSize > 0. If you need empty payloads, decide a policy.
        if (shardSize == 0) {
            // simplest policy: represent empty input as 1-byte shards (all zeros).
            shardSize = 1;
        }

        byte[][] shards = new byte[totalShards][shardSize];

        // Fill data shards (pad zeros automatically)
        int offset = 0;
        for (int i = 0; i < dataShards; i++) {
            int bytesToCopy = Math.min(shardSize, data.length - offset);
            if (bytesToCopy > 0) {
                System.arraycopy(data, offset, shards[i], 0, bytesToCopy);
                offset += bytesToCopy;
            }
        }

        ReedSolomon rs = ReedSolomon.create(dataShards, parityShards);
        rs.encodeParity(shards, 0, shardSize);

        return shards;
    }
}
