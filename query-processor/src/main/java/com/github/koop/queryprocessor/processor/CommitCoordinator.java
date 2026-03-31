package com.github.koop.queryprocessor.processor;

import com.github.koop.common.messages.Message;
import com.github.koop.common.messages.Message.FileCommitMessage;
import com.github.koop.common.messages.Message.MultipartCommitMessage;
import com.github.koop.common.pubsub.CommitTopics;
import com.github.koop.common.pubsub.PubSubClient;

import io.javalin.Javalin;
import io.javalin.http.Context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinates the two-phase commit for PUT operations on the Query Processor side.
 *
 * <h2>Protocol</h2>
 * <ol>
 *   <li>Caller invokes {@link #beginCommit} which:
 *       <ul>
 *         <li>Registers an in-flight {@link PendingCommit} keyed by {@code requestId}.</li>
 *         <li>Publishes a {@link FileCommitMessage} (or {@link MultipartCommitMessage})
 *             to the per-partition Kafka topic ({@code "partition-N"}) so every Storage
 *             Node for that partition receives the commit command.</li>
 *       </ul>
 *   </li>
 *   <li>Each SN that successfully commits POSTs to {@code /ack/{requestId}} on
 *       the Javalin HTTP server embedded in this coordinator.</li>
 *   <li>{@link #beginCommit} blocks until {@value #QUORUM} ACKs arrive or the
 *       timeout elapses, then returns {@code true}/{@code false}.</li>
 * </ol>
 *
 * <p>The Javalin server is started once at construction time and shared across
 * all concurrent PUT operations — each pending commit is identified by its
 * {@code requestId} so concurrent operations do not interfere.
 */
public final class CommitCoordinator implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    /** Number of SN ACKs required before reporting success to the caller. */
    public static final int QUORUM = 7;

    /** Total number of storage nodes in one erasure set. */
    public static final int TOTAL_NODES = 9;

    /**
     * How long (in seconds) to wait for quorum before declaring failure.
     * SNs that missed the data stream need time to reconstruct from peers.
     */
    private static final int ACK_TIMEOUT_SECONDS = 30;

    private static final Logger logger = LogManager.getLogger(CommitCoordinator.class);

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Map from requestId to pending commit state. Thread-safe by ConcurrentHashMap. */
    private final ConcurrentHashMap<String, PendingCommit> inFlight = new ConcurrentHashMap<>();

    private final PubSubClient pubSubClient;
    private final Javalin ackServer;
    private final InetSocketAddress ackAddress;
    private final int ackTimeoutSeconds;

    // -----------------------------------------------------------------------
    // Construction / lifecycle
    // -----------------------------------------------------------------------

    /**
     * @param pubSubClient a started {@link PubSubClient} backed by Kafka (or
     *                     {@link com.github.koop.common.pubsub.MemoryPubSub} in tests).
     * @param ackPort      the port this QP node should listen on for SN ACKs.
     *                     Pass {@code 0} to let the OS pick a free port.
     */
    public CommitCoordinator(PubSubClient pubSubClient, int ackPort) {
        this(pubSubClient, ackPort, ACK_TIMEOUT_SECONDS);
    }

    /**
     * Full constructor allowing a custom ACK timeout. Prefer the two-arg
     * constructor in production; this overload exists for tests that need a
     * short timeout to avoid long waits on expected failures.
     *
     * @param ackTimeoutSeconds how long to wait for quorum before timing out.
     */
    public CommitCoordinator(PubSubClient pubSubClient, int ackPort, int ackTimeoutSeconds) {
        this.pubSubClient = pubSubClient;
        this.ackTimeoutSeconds = ackTimeoutSeconds;

        this.ackServer = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.startup.showJavalinBanner = false;
            config.routes.post("/ack/{requestId}", this::handleAck);
        });

        this.ackServer.start(ackPort);

        this.ackAddress = new InetSocketAddress(
                resolveLocalHostname(),
                this.ackServer.port());

        logger.info("CommitCoordinator ACK server listening on {}", ackAddress);
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Publishes a single-part commit command and blocks until a quorum of Storage
     * Nodes ACK the commit (or the timeout expires).
     *
     * @param requestId the UUID that was used for the preceding shard uploads.
     * @param partition the partition number — used as the Kafka topic via
     *                  {@link CommitTopics#forPartition}.
     * @param bucket    object bucket.
     * @param key       object key.
     * @return {@code true} iff at least {@value #QUORUM} SNs ACKed within the timeout.
     */
    public boolean beginCommit(UUID requestId, int partition, String bucket, String key) {
        return runCommit(requestId, () -> {
            FileCommitMessage msg = new FileCommitMessage(
                    bucket, key, requestId.toString(), ackAddress);
            String topic = CommitTopics.forPartition(partition);
            pubSubClient.pub(topic, Message.serializeMessage(msg));
            logger.debug("Published FileCommitMessage for requestId {} on topic {}", requestId, topic);
        });
    }

    /**
     * Publishes a multipart commit command and blocks until quorum.
     *
     * @param requestId the UUID for the upload.
     * @param partition the partition number — used as the Kafka topic via
     *                  {@link CommitTopics#forPartition}.
     * @param bucket    object bucket.
     * @param key       object key.
     * @param chunks    ordered list of part/chunk identifiers.
     * @return {@code true} iff at least {@value #QUORUM} SNs ACKed within the timeout.
     */
    public boolean beginMultipartCommit(UUID requestId, int partition, String bucket, String key, List<String> chunks) {
        return runCommit(requestId, () -> {
            MultipartCommitMessage msg = new MultipartCommitMessage(
                    bucket, key, requestId.toString(), ackAddress, chunks);
            String topic = CommitTopics.forPartition(partition);
            pubSubClient.pub(topic, Message.serializeMessage(msg));
            logger.debug("Published MultipartCommitMessage for requestId {} on topic {}", requestId, topic);
        });
    }

    @Override
    public void close() {
        ackServer.stop();
        logger.info("CommitCoordinator ACK server stopped");
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Javalin handler for {@code POST /ack/{requestId}}.
     *
     * <p>Called by each Storage Node once it has committed the operation to its
     * op-log and metadata store. Decrements the {@link CountDownLatch} of the
     * matching in-flight commit, waking the blocked caller when quorum is reached.
     */
    private void handleAck(Context ctx) {
        String requestId = ctx.pathParam("requestId");

        PendingCommit commit = inFlight.get(requestId);
        if (commit == null) {
            // Unknown or already-completed request — ignore gracefully.
            logger.warn("Received ACK for unknown requestId {}", requestId);
            ctx.status(404);
            return;
        }

        int acks = commit.ackCount.incrementAndGet();
        logger.trace("ACK {}/{} received for requestId {}", acks, TOTAL_NODES, requestId);

        // Release one permit on the latch; the committer thread wakes up as
        // soon as QUORUM permits have been released. Extra ACKs (from nodes
        // beyond QUORUM) call countDown() on an already-zero latch — safe no-op.
        commit.latch.countDown();

        ctx.status(200);
    }

    /**
     * Core template used by both {@link #beginCommit} and {@link #beginMultipartCommit}.
     *
     * <ol>
     *   <li>Registers the pending commit <em>before</em> publishing, so no ACK can
     *       arrive before the entry exists in {@link #inFlight}.</li>
     *   <li>Runs {@code publishAction} to send the Kafka/pubsub message.</li>
     *   <li>Waits up to {@link #ackTimeoutSeconds}s for {@value #QUORUM} ACKs.</li>
     *   <li>Cleans up the in-flight entry regardless of outcome.</li>
     * </ol>
     */
    private boolean runCommit(UUID requestId, ThrowingRunnable publishAction) {
        String id = requestId.toString();

        PendingCommit commit = new PendingCommit(QUORUM);
        inFlight.put(id, commit);

        try {
            publishAction.run();

            boolean quorumReached = commit.latch.await(ackTimeoutSeconds, TimeUnit.SECONDS);

            if (quorumReached) {
                logger.info("Quorum reached for requestId {} ({} ACKs)", id, commit.ackCount.get());
            } else {
                logger.warn("Timeout waiting for quorum on requestId {} (got {}/{})",
                        id, commit.ackCount.get(), QUORUM);
            }
            return quorumReached;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted waiting for ACKs on requestId {}", id);
            return false;
        } catch (Exception e) {
            logger.error("Error during commit for requestId {}: {}", id, e.getMessage(), e);
            return false;
        } finally {
            // Always remove so late ACKs get 404 rather than matching a future
            // request that happens to reuse the same UUID (defence-in-depth).
            inFlight.remove(id);
        }
    }

    /**
     * Returns a hostname that Storage Nodes can reach to POST their ACKs.
     * Falls back to loopback in environments where the local hostname cannot
     * be resolved (e.g. some CI containers).
     */
    private static String resolveLocalHostname() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("Could not resolve local hostname, falling back to loopback");
            return "127.0.0.1";
        }
    }

    // -----------------------------------------------------------------------
    // Inner types
    // -----------------------------------------------------------------------

    /** Mutable state for one in-flight commit operation. */
    private static final class PendingCommit {
        final CountDownLatch latch;
        /** Total ACKs received so far — may exceed QUORUM (informational only). */
        final AtomicInteger ackCount = new AtomicInteger(0);

        PendingCommit(int quorum) {
            this.latch = new CountDownLatch(quorum);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}