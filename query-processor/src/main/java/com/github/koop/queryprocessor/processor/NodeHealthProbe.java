package com.github.koop.queryprocessor.processor;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.ErasureSetConfiguration.ErasureSet;
import com.github.koop.common.metadata.ErasureSetConfiguration.Machine;
import com.github.koop.common.metadata.MetadataClient;

/**
 * Background loop that periodically probes every known storage node via
 * {@code GET /health} and feeds the result into a {@link NodeHealthTracker}.
 *
 * <p>The probe is the only mechanism that <em>actively</em> discovers node
 * recovery — the data path also records outcomes but does not retry dead
 * nodes on its own. Probe ticks never overlap (fixed-delay scheduling) and
 * fan out per-node requests asynchronously so one slow node never starves
 * the rest of the tick.
 */
public final class NodeHealthProbe implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(NodeHealthProbe.class);

    private static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(5);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

    private final NodeHealthTracker tracker;
    private final HttpClient httpClient;
    private final MetadataClient metadataClient;
    private final Duration probeInterval;
    private final Duration probeTimeout;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public NodeHealthProbe(NodeHealthTracker tracker,
                           HttpClient httpClient,
                           MetadataClient metadataClient) {
        this(tracker, httpClient, metadataClient, DEFAULT_INTERVAL, DEFAULT_TIMEOUT);
    }

    public NodeHealthProbe(NodeHealthTracker tracker,
                           HttpClient httpClient,
                           MetadataClient metadataClient,
                           Duration probeInterval,
                           Duration probeTimeout) {
        if (tracker == null) throw new IllegalArgumentException("tracker is required");
        if (httpClient == null) throw new IllegalArgumentException("httpClient is required");
        if (metadataClient == null) throw new IllegalArgumentException("metadataClient is required");
        if (probeInterval == null || probeInterval.isNegative() || probeInterval.isZero()) {
            throw new IllegalArgumentException("probeInterval must be positive");
        }
        if (probeTimeout == null || probeTimeout.isNegative() || probeTimeout.isZero()) {
            throw new IllegalArgumentException("probeTimeout must be positive");
        }
        this.tracker = tracker;
        this.httpClient = httpClient;
        this.metadataClient = metadataClient;
        this.probeInterval = probeInterval;
        this.probeTimeout = probeTimeout;

        ThreadFactory factory = r -> Thread.ofVirtual().name("node-health-probe").unstarted(r);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(factory);
    }

    /**
     * Starts the periodic probe. Idempotent calls after the first throw
     * {@link IllegalStateException} to prevent silent double-scheduling.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("NodeHealthProbe already started");
        }
        // Listener exists so the tracker is wired into the metadata stream;
        // each tick re-reads the latest snapshot so we don't need to act here.
        metadataClient.listen(ErasureSetConfiguration.class, (prev, curr) -> { });

        long delayMs = probeInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::tick, 0L, delayMs, TimeUnit.MILLISECONDS);
        logger.info("NodeHealthProbe started (interval={}ms, timeout={}ms)",
                delayMs, probeTimeout.toMillis());
    }

    /**
     * Executes one probe pass synchronously. Exposed package-private so tests
     * can drive ticks deterministically without waiting for the scheduler.
     */
    void tick() {
        try {
            Set<InetSocketAddress> nodes = snapshotInventory();
            if (nodes.isEmpty()) {
                return;
            }
            List<CompletableFuture<Void>> futures = nodes.stream()
                    .map(this::probeNode)
                    .toList();
            long overallDeadlineMs = probeTimeout.toMillis() + 1000L;
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .orTimeout(overallDeadlineMs, TimeUnit.MILLISECONDS)
                    .exceptionally(t -> null) // already recorded per-node; swallow aggregate
                    .join();
        } catch (Throwable t) {
            logger.warn("NodeHealthProbe tick failed: {}", t.toString());
        }
    }

    private Set<InetSocketAddress> snapshotInventory() {
        ErasureSetConfiguration cfg = metadataClient.get(ErasureSetConfiguration.class);
        if (cfg == null || cfg.getErasureSets() == null) {
            return Set.of();
        }
        Set<InetSocketAddress> addrs = new HashSet<>();
        for (ErasureSet set : cfg.getErasureSets()) {
            if (set.getMachines() == null) continue;
            for (Machine m : set.getMachines()) {
                addrs.add(new InetSocketAddress(m.getIp(), m.getPort()));
            }
        }
        return addrs;
    }

    private CompletableFuture<Void> probeNode(InetSocketAddress addr) {
        HttpRequest req;
        try {
            req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + addr.getHostString() + ":" + addr.getPort() + "/health"))
                    .timeout(probeTimeout)
                    .GET()
                    .build();
        } catch (Exception e) {
            tracker.recordFailure(addr);
            return CompletableFuture.completedFuture(null);
        }
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.discarding())
                .handle((resp, err) -> {
                    if (err != null) {
                        tracker.recordFailure(addr);
                    } else if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                        tracker.recordSuccess(addr);
                    } else {
                        tracker.recordFailure(addr);
                    }
                    return null;
                });
    }

    /**
     * Stops the probe. Idempotent. Best-effort wait of up to 2s for the
     * scheduler thread to terminate.
     */
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                logger.warn("NodeHealthProbe scheduler did not terminate within 2s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("NodeHealthProbe stopped");
    }

    @Override
    public void close() {
        shutdown();
    }
}
