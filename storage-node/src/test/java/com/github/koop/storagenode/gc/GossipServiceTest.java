package com.github.koop.storagenode.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.RocksDbStorageStrategy;

/**
 * Verifies that two GossipService instances sharing a PubSub bus converge on a
 * global watermark equal to the minimum of their local minimums.
 */
public class GossipServiceTest {

    private static final int PARTITION = 3;

    @TempDir
    Path dirA;
    @TempDir
    Path dirB;

    private RocksDbStorageStrategy stratA;
    private RocksDbStorageStrategy stratB;
    private Database dbA;
    private Database dbB;
    private PubSubClient bus;
    private GossipService gossipA;
    private GossipService gossipB;
    private PartitionWatermarks wmA;
    private PartitionWatermarks wmB;

    @BeforeEach
    public void setup() throws Exception {
        stratA = new RocksDbStorageStrategy(dirA.resolve("db").toAbsolutePath().toString());
        stratB = new RocksDbStorageStrategy(dirB.resolve("db").toAbsolutePath().toString());
        dbA = new Database(stratA);
        dbB = new Database(stratB);

        bus = new PubSubClient(new MemoryPubSub());
        bus.start();

        wmA = new PartitionWatermarks();
        wmB = new PartitionWatermarks();
        ActiveReadTracker readsA = new ActiveReadTracker();
        ActiveReadTracker readsB = new ActiveReadTracker();

        gossipA = new GossipService("A", dbA, readsA, wmA, bus,
                ignored -> Set.of(PARTITION), 60_000L);
        gossipB = new GossipService("B", dbB, readsB, wmB, bus,
                ignored -> Set.of(PARTITION), 60_000L);
    }

    @AfterEach
    public void teardown() throws Exception {
        dbA.close();
        dbB.close();
        bus.close();
    }

    @Test
    public void gossipConvergesToMinOfPeers() throws Exception {
        // A has progressed to seq=100, B is still at 30.
        dbA.putItem("bkt/x", PARTITION, 100L, "rA", 0L);
        dbB.putItem("bkt/y", PARTITION, 30L, "rB", 0L);

        // Both nodes need to be subscribed to the shared cluster topic before either
        // broadcasts payloads the other should see. MemoryPubSub only delivers to
        // existing subscribers, so we tick twice: round 1 sets up subscriptions,
        // round 2 exchanges payloads each side will actually receive.
        gossipA.tick();
        gossipB.tick();
        gossipA.tick();
        gossipB.tick();

        assertTrue(wmA.watermarkFor(PARTITION, 60_000L).isPresent());
        assertEquals(30L, wmA.watermarkFor(PARTITION, 60_000L).getAsLong());
        assertTrue(wmB.watermarkFor(PARTITION, 60_000L).isPresent());
        assertEquals(30L, wmB.watermarkFor(PARTITION, 60_000L).getAsLong());
    }

    @Test
    public void activeReadLowersLocalMinimum() throws Exception {
        dbA.putItem("bkt/z", PARTITION, 100L, "rA", 0L);

        ActiveReadTracker readsA = new ActiveReadTracker();
        PartitionWatermarks wmA2 = new PartitionWatermarks();
        GossipService gA = new GossipService("A2", dbA, readsA, wmA2, bus,
                ignored -> Set.of(PARTITION), 60_000L);

        try (var ignored = readsA.begin(PARTITION, 42L)) {
            gA.tick();
        }

        // Even though A's partition seq is 100, the active read pins min to 42.
        assertEquals(42L, wmA2.watermarkFor(PARTITION, 60_000L).getAsLong());
    }
}
