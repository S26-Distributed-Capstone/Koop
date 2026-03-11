package com.github.koop.queryprocessor.processor;

import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.metadata.ReplicaSetConfiguration;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Routes a storage key to a partition number and then to the list of storage
 * node addresses responsible for that partition, driven entirely by live
 * metadata rather than hardcoded boundaries.
 *
 * Partition assignment:
 *   Count the total number of partitions declared across all
 *   {@link PartitionSpreadConfiguration.PartitionSpread} entries.
 *   Hash the key with CRC32 and mod by that total to get an index.
 *   Walk the spread entries to find which partition number sits at that index.
 *
 * Node resolution:
 *   Find which spread entry's partition list contains the partition number.
 *   Read that entry's {@code erasure_set} name (e.g. {@code "set2"}).
 *   Parse the numeric suffix to find the matching {@link ReplicaSetConfiguration.ReplicaSet}.
 *   Map its machines to {@link InetSocketAddress} instances.
 */
public final class ErasureRouting {

    private final PartitionSpreadConfiguration partitionSpread;
    private final ReplicaSetConfiguration replicaSetConfig;

    public ErasureRouting(PartitionSpreadConfiguration partitionSpread,
                          ReplicaSetConfiguration replicaSetConfig) {
        if (partitionSpread == null)
            throw new IllegalArgumentException("partitionSpread is null");
        if (replicaSetConfig == null)
            throw new IllegalArgumentException("replicaSetConfig is null");
        this.partitionSpread = partitionSpread;
        this.replicaSetConfig = replicaSetConfig;
    }

    /**
     * Returns the partition number that owns {@code key}.
     *
     * Total partition count = sum of partition list sizes across all spread
     * entries. CRC32(key) mod total gives an index into that flat space; we
     * then find the partition number at that position.
     */
    public int getPartition(String key) {
        var spreads = partitionSpread.getPartitionSpread();
        if (spreads == null || spreads.isEmpty())
            throw new IllegalStateException("PartitionSpreadConfiguration has no entries");

        int total = spreads.stream()
                .mapToInt(s -> s.getPartitions().size())
                .sum();

        CRC32 crc = new CRC32();
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        crc.update(b, 0, b.length);
        int index = (int) (crc.getValue() % total);

        int offset = 0;
        for (var spread : spreads) {
            var partitions = spread.getPartitions();
            if (index < offset + partitions.size()) {
                return partitions.get(index - offset);
            }
            offset += partitions.size();
        }

        throw new IllegalStateException("Failed to resolve partition for index " + index);
    }

    /**
     * Returns the node addresses responsible for {@code partition}.
     *
     * Finds which spread entry owns the partition, reads its
     * {@code erasure_set} name (e.g. {@code "set2"}), parses the numeric
     * suffix to identify the replica set, and maps that set's machines to
     * {@link InetSocketAddress} instances.
     */
    public List<InetSocketAddress> getNodes(int partition) {
        var spreads = partitionSpread.getPartitionSpread();
        if (spreads == null || spreads.isEmpty())
            throw new IllegalStateException("PartitionSpreadConfiguration has no entries");

        for (var spread : spreads) {
            if (spread.getPartitions().contains(partition)) {
                int setNumber = parseSetNumber(spread.getErasureSet());
                return replicaSetConfig.getReplicaSets().stream()
                        .filter(rs -> rs.getNumber() == setNumber)
                        .findFirst()
                        .map(rs -> rs.getMachines().stream()
                                .map(m -> new InetSocketAddress(m.getIp(), m.getPort()))
                                .toList())
                        .orElseThrow(() -> new IllegalStateException(
                                "No replica set found for set number: " + setNumber));
            }
        }

        throw new IllegalStateException("No erasure set found for partition " + partition);
    }

    /**
     * Parses the numeric suffix from an erasure set name, e.g. {@code "set2"} → {@code 2}.
     */
    private static int parseSetNumber(String erasureSetName) {
        try {
            return Integer.parseInt(erasureSetName.replaceAll("[^0-9]", ""));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Cannot parse set number from erasure set name: " + erasureSetName);
        }
    }
}