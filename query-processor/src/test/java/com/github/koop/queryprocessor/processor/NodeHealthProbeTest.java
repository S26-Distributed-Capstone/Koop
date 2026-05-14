package com.github.koop.queryprocessor.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.ErasureSetConfiguration.ErasureSet;
import com.github.koop.common.metadata.ErasureSetConfiguration.Machine;
import com.github.koop.common.metadata.MemoryFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.queryprocessor.processor.NodeHealthTracker.NodeStatus;

import io.javalin.Javalin;

class NodeHealthProbeTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private MemoryFetcher fetcher;
    private MetadataClient metadataClient;
    private HttpClient httpClient;
    private NodeHealthTracker tracker;

    @BeforeEach
    void setUp() {
        fetcher = new MemoryFetcher();
        metadataClient = new MetadataClient(fetcher);
        metadataClient.start();
        httpClient = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        tracker = new NodeHealthTracker();
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : closeables) {
            try { c.close(); } catch (Exception ignored) { }
        }
        closeables.clear();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Start a Javalin server with the requested behavior; auto-closed in tearDown. */
    private InetSocketAddress startServer(boolean healthy) {
        Javalin app = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            if (healthy) {
                config.routes.get("/health", ctx -> ctx.status(200).result("OK"));
            }
            // when !healthy, no /health route → Javalin returns 404 → recorded as failure
        });
        app.start(0);
        closeables.add(app::stop);
        return new InetSocketAddress("127.0.0.1", app.port());
    }

    private void publishConfig(InetSocketAddress... nodes) {
        ErasureSetConfiguration cfg = new ErasureSetConfiguration();
        ErasureSet set = new ErasureSet();
        set.setNumber(1);
        set.setN(nodes.length);
        set.setM(Math.max(1, nodes.length - 1));
        set.setWriteQuorum(nodes.length);
        List<Machine> machines = new ArrayList<>();
        for (InetSocketAddress n : nodes) {
            Machine m = new Machine();
            m.setIp(n.getHostString());
            m.setPort(n.getPort());
            machines.add(m);
        }
        set.setMachines(machines);
        cfg.setErasureSets(List.of(set));
        fetcher.update(cfg);
    }

    private NodeHealthProbe newProbe() {
        NodeHealthProbe probe = new NodeHealthProbe(
                tracker, httpClient, metadataClient,
                Duration.ofSeconds(1), Duration.ofMillis(500));
        closeables.add(probe);
        return probe;
    }

    // -------------------------------------------------------------------------
    // Reachable / unreachable node behaviour (single tick)
    // -------------------------------------------------------------------------

    @Test
    void probe_marksReachableNodeHealthy() {
        InetSocketAddress addr = startServer(true);
        publishConfig(addr);
        NodeHealthProbe probe = newProbe();

        probe.tick();

        assertNotNull(tracker.getHealth(addr));
        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(addr).status());
        assertTrue(tracker.isHealthy(addr));
    }

    @Test
    void probe_marksUnreachableNodeFailure() {
        InetSocketAddress addr = startServer(false); // server up, but no /health → 404
        publishConfig(addr);
        NodeHealthProbe probe = newProbe();

        probe.tick();
        assertEquals(NodeStatus.SUSPECT, tracker.getHealth(addr).status(),
                "One failure should produce SUSPECT");

        probe.tick();
        assertEquals(NodeStatus.DOWN, tracker.getHealth(addr).status(),
                "Two consecutive failures should produce DOWN");
        assertFalse(tracker.isHealthy(addr));
    }

    // -------------------------------------------------------------------------
    // Recovery: success after DOWN promotes to HEALTHY
    // -------------------------------------------------------------------------

    @Test
    void probe_recordsRecovery() {
        InetSocketAddress addr = startServer(true);
        publishConfig(addr);
        NodeHealthProbe probe = newProbe();

        // Force the node into DOWN before any probe runs.
        tracker.recordFailure(addr);
        tracker.recordFailure(addr);
        assertEquals(NodeStatus.DOWN, tracker.getHealth(addr).status());

        probe.tick();

        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(addr).status());
        assertEquals(0, tracker.getHealth(addr).consecutiveFailures());
        assertTrue(tracker.isHealthy(addr));
    }

    // -------------------------------------------------------------------------
    // Metadata updates: new nodes picked up on the next tick
    // -------------------------------------------------------------------------

    @Test
    void probe_picksUpNewNodesFromMetadata() {
        InetSocketAddress first = startServer(true);
        publishConfig(first);
        NodeHealthProbe probe = newProbe();

        probe.tick();
        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(first).status());

        // Metadata gets a second node — probe should pick it up next tick.
        InetSocketAddress second = startServer(true);
        publishConfig(first, second);

        probe.tick();

        assertNotNull(tracker.getHealth(second), "Second node should be probed after metadata update");
        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(second).status());
    }

    @Test
    void probe_skipsTickWhenNoConfigurationPublished() {
        // No publishConfig() call → metadataClient.get() returns null
        NodeHealthProbe probe = newProbe();

        probe.tick(); // should not throw

        assertEquals(0, tracker.getAllTrackedNodes().size());
    }

    // -------------------------------------------------------------------------
    // Lifecycle: start / shutdown
    // -------------------------------------------------------------------------

    @Test
    void probe_scheduledTickRunsAutomatically() throws InterruptedException {
        InetSocketAddress addr = startServer(true);
        publishConfig(addr);
        NodeHealthProbe probe = newProbe();

        probe.start();

        // Probe interval is 1s; first tick fires immediately. Allow generous slack.
        long deadline = System.currentTimeMillis() + 3000;
        while (tracker.getHealth(addr) == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertNotNull(tracker.getHealth(addr), "Scheduled tick should record health within 3s");
        assertEquals(NodeStatus.HEALTHY, tracker.getHealth(addr).status());
    }

    @Test
    void probe_shutdownIsIdempotent() {
        NodeHealthProbe probe = newProbe();
        probe.start();

        probe.shutdown();
        // Second call must not throw.
        probe.shutdown();
    }

    @Test
    void probe_doubleStartThrows() {
        NodeHealthProbe probe = newProbe();
        probe.start();
        assertThrows(IllegalStateException.class, probe::start);
    }

    // -------------------------------------------------------------------------
    // Per-tick fan-out: one slow node does not block recording for others
    // -------------------------------------------------------------------------

    @Test
    void probe_recordsAllReachableNodesInOneTick() {
        AtomicInteger reachableCount = new AtomicInteger();
        List<InetSocketAddress> addrs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            addrs.add(startServer(true));
            reachableCount.incrementAndGet();
        }
        publishConfig(addrs.toArray(new InetSocketAddress[0]));
        NodeHealthProbe probe = newProbe();

        probe.tick();

        for (InetSocketAddress addr : addrs) {
            assertNotNull(tracker.getHealth(addr));
            assertEquals(NodeStatus.HEALTHY, tracker.getHealth(addr).status());
        }
        assertEquals(reachableCount.get(), tracker.getStatusCounts().getOrDefault(NodeStatus.HEALTHY, 0L).intValue());
    }
}
