package com.github.koop.queryprocessor.processor;

import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.zip.CRC32;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Routes a storage key to a partition number and then to the list of storage
 * node addresses responsible for that partition, driven entirely by live
 * metadata rather than hardcoded boundaries.
 *
 * <p>
 * Partition assignment:
 * <ol>
 * <li>Collect all partition numbers from all spread entries and sort them.</li>
 * <li>Hash the key with CRC32 and mod by the total count to get an index.</li>
 * <li>Look up the partition number at that index in the sorted list.</li>
 * </ol>
 *
 * <p>
 * Sorting ensures that the same key always maps to the same partition number
 * regardless of the order in which partitions appear in the config or how they
 * are redistributed across erasure sets.
 *
 * <p>
 * Errors are logged and empty Optionals returned rather than throwing, so
 * that a bad config entry does not crash the query processor.
 */
public final class ErasureRouting {

    private static final Logger logger = LogManager.getLogger(ErasureRouting.class);

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
     * Returns the partition number that owns {@code key}, or an empty
     * {@link OptionalInt} if routing cannot be resolved.
     *
     * <p>
     * All partition numbers across all spread entries are collected and
     * sorted so that the mapping is stable even if partition assignments are
     * redistributed between erasure sets.
     */
    public OptionalInt getPartition(String key) {
        var spreads = partitionSpread.getPartitionSpread();
        if (spreads == null || spreads.isEmpty()) {
            logger.error("getPartition failed: PartitionSpreadConfiguration has no entries");
            return OptionalInt.empty();
        }

        // Collect and sort all partition numbers for a stable, order-independent
        // mapping
        List<Integer> allPartitions = spreads.stream()
                .flatMap(s -> s.getPartitions().stream())
                .sorted()
                .toList();

        if (allPartitions.isEmpty()) {
            logger.error("getPartition failed: no partitions found in PartitionSpreadConfiguration");
            return OptionalInt.empty();
        }

        CRC32 crc = new CRC32();
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        crc.update(b, 0, b.length);
        int index = (int) (crc.getValue() % allPartitions.size());

        return OptionalInt.of(allPartitions.get(index));
    }

    /**
     * Returns the ErasureSet responsible for {@code partition}, or an empty
     * {@link Optional} if it cannot be resolved.
     */
    public Optional<ErasureSetConfiguration.ErasureSet> getErasureSet(int partition) {
        var spreads = partitionSpread.getPartitionSpread();
        if (spreads == null || spreads.isEmpty()) {
            logger.error("getErasureSet failed: PartitionSpreadConfiguration has no entries");
            return Optional.empty();
        }

        for (var spread : spreads) {
            if (spread.getPartitions().contains(partition)) {
                int setNumber = spread.getErasureSet();

                Optional<ErasureSetConfiguration.ErasureSet> erasureSet = erasureSetConfig.getErasureSets().stream()
                        .filter(es -> es.getNumber() == setNumber)
                        .findFirst();

                if (erasureSet.isEmpty()) {
                    logger.error("getErasureSet failed: no erasure set found for set number {}", setNumber);
                }
                return erasureSet;
            }
        }

        logger.error("getErasureSet failed: no spread entry found for partition {}", partition);
        return Optional.empty();
    }
}