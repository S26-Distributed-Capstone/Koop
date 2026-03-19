package com.github.koop.queryprocessor.processor;

import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Routes a storage key to a partition number and then to the list of storage
 * node addresses responsible for that partition, driven entirely by live
 * metadata rather than hardcoded boundaries.
 *
 * <p>Partition assignment:
 * <ol>
 *   <li>Collect all partition numbers from all spread entries and sort them.</li>
 *   <li>Hash the key with CRC32 and mod by the total count to get an index.</li>
 *   <li>Look up the partition number at that index in the sorted list.</li>
 * </ol>
 *
 * <p>Sorting ensures that the same key always maps to the same partition number
 * regardless of the order in which partitions appear in the config or how they
 * are redistributed across erasure sets.
 *
 * <p>Errors are logged and sentinel values returned rather than throwing, so
 * that a bad config entry does not crash the query processor.
 */
public final class ErasureRouting {

    private static final Logger logger = LogManager.getLogger(ErasureRouting.class);

    /** Returned by {@link #getPartition} when routing cannot be resolved. */
    public static final int INVALID_PARTITION = -1;

    /** Returned by {@link #getNodes} when nodes cannot be resolved. */
    public static final List<InetSocketAddress> INVALID_NODES = Collections.emptyList();

    private final PartitionSpreadConfiguration partitionSpread;
    private final ErasureSetConfiguration erasureSetConfig;

    public ErasureRouting(PartitionSpreadConfiguration partitionSpread,
                          ErasureSetConfiguration erasureSetConfig) {
        if (partitionSpread == null)
            throw new IllegalArgumentException("partitionSpread is null");
        if (erasureSetConfig == null)
            throw new IllegalArgumentException("erasureSetConfig is null");
        this.partitionSpread = partitionSpread;
        this.erasureSetConfig = erasureSetConfig;
    }

    /**
     * Returns the partition number that owns {@code key}, or
     * {@link #INVALID_PARTITION} if routing cannot be resolved.
     *
     * <p>All partition numbers across all spread entries are collected and
     * sorted so that the mapping is stable even if partition assignments are
     * redistributed between erasure sets.
     */
    public int getPartition(String key) {
        var spreads = partitionSpread.getPartitionSpread();
        if (spreads == null || spreads.isEmpty()) {
            logger.error("getPartition failed: PartitionSpreadConfiguration has no entries");
            return INVALID_PARTITION;
        }

        // Collect and sort all partition numbers for a stable, order-independent mapping
        List<Integer> allPartitions = spreads.stream()
                .flatMap(s -> s.getPartitions().stream())
                .sorted()
                .toList();

        if (allPartitions.isEmpty()) {
            logger.error("getPartition failed: no partitions found in PartitionSpreadConfiguration");
            return INVALID_PARTITION;
        }

        CRC32 crc = new CRC32();
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        crc.update(b, 0, b.length);
        int index = (int) (crc.getValue() % allPartitions.size());

        return allPartitions.get(index);
    }

    /**
     * Returns the node addresses responsible for {@code partition}, or
     * {@link #INVALID_NODES} if nodes cannot be resolved.
     *
     * <p>Finds which spread entry owns the partition, reads its {@code erasure_set}
     * number directly, looks up the matching {@link ErasureSetConfiguration.ErasureSet},
     * and maps its machines to {@link InetSocketAddress} instances.
     */
    public List<InetSocketAddress> getNodes(int partition) {
        if (partition == INVALID_PARTITION) {
            logger.error("getNodes called with INVALID_PARTITION");
            return INVALID_NODES;
        }

        var spreads = partitionSpread.getPartitionSpread();
        if (spreads == null || spreads.isEmpty()) {
            logger.error("getNodes failed: PartitionSpreadConfiguration has no entries");
            return INVALID_NODES;
        }

        for (var spread : spreads) {
            if (spread.getPartitions().contains(partition)) {
                int setNumber = spread.getErasureSet();

                var nodes = erasureSetConfig.getErasureSets().stream()
                        .filter(es -> es.getNumber() == setNumber)
                        .findFirst()
                        .map(es -> es.getMachines().stream()
                                .map(m -> new InetSocketAddress(m.getIp(), m.getPort()))
                                .toList())
                        .orElse(null);

                if (nodes == null) {
                    logger.error("getNodes failed: no erasure set found for set number {}", setNumber);
                    return INVALID_NODES;
                }
                return nodes;
            }
        }

        logger.error("getNodes failed: no spread entry found for partition {}", partition);
        return INVALID_NODES;
    }
}