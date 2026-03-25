package com.github.koop.queryprocessor.processor;

import com.github.koop.common.messages.Message;
import com.github.koop.common.messages.Message.FileCommitMessage;
import com.github.koop.common.messages.Message.MultipartCommitMessage;
import com.github.koop.common.pubsub.PubSubClient;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
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
 *         <li>Publishes a {@link FileCommitMessage} (or {@link MultipartCommitMessage}) to
 *             the Kafka topic so every Storage Node receives the commit command.</li>
 *       </ul>
 *   </li>
 *   <li>Each SN that successfully commits sends an HTTP POST to
 *       {@code /ack/{requestId}} on this server.</li>
 *   <li>{@link #beginCommit} blocks until {@value #QUORUM} ACKs arrive or the
 *       timeout elapses, then returns {@code true}/{@code false}.</li>
 * </ol>
 *
 * <p>The embedded {@link HttpServer} is started once at construction time and
 * shared across all concurrent PUT operations — each pending commit is identified
 * by its {@code requestId} so concurrent operations do not interfere.
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

    /**
     * Derives the Kafka topic for a commit from the partition number.
     * Each partition has its own topic so SNs only consume messages relevant
     * to their own partition, and so topic-level ordering is preserved per partition.
     */
    static String topicFor(int partition) {
        return "partition-" + partition;
    }

    private static final Logger logger = LogManager.getLogger(CommitCoordinator.class);

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Map from requestId -> pending commit state. Thread-safe by ConcurrentHashMap. */
    private final ConcurrentHashMap<String, PendingCommit> inFlight = new ConcurrentHashMap<>();

    private final PubSubClient pubSubClient;
    private final HttpServer ackServer;
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
    public CommitCoordinator(PubSubClient pubSubClient, int ackPort) throws IOException {
        this(pubSubClient, ackPort, ACK_TIMEOUT_SECONDS);
    }

    /**
     * Full constructor allowing a custom ACK timeout. Prefer the two-arg
     * constructor in production; this overload exists for tests that need a
     * short timeout to avoid long waits on expected failures.
     *
     * @param ackTimeoutSeconds how long to wait for quorum before timing out.
     */
    public CommitCoordinator(PubSubClient pubSubClient, int ackPort, int ackTimeoutSeconds) throws IOException {
        this.ackTimeoutSeconds = ackTimeoutSeconds;
        this.pubSubClient = pubSubClient;

        // Bind the ACK server. Using a virtual-thread executor so that many
        // concurrent ACK requests (one per SN per in-flight PUT) never block.
        this.ackServer = HttpServer.create(new InetSocketAddress(ackPort), /*backlog=*/ 64);
        this.ackServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.ackServer.createContext("/ack/", exchange -> {
            try {
                // URI is /ack/{requestId}
                String path = exchange.getRequestURI().getPath();          // e.g. "/ack/abc-123"
                String requestId = path.substring("/ack/".length());

                PendingCommit commit = inFlight.get(requestId);
                if (commit == null) {
                    // Unknown or already-completed request — ignore gracefully.
                    logger.warn("Received ACK for unknown requestId {}", requestId);
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }

                int acks = commit.ackCount.incrementAndGet();
                logger.trace("ACK {}/{} received for requestId {}", acks, TOTAL_NODES, requestId);

                // Release one permit on the latch; the committer thread wakes
                // up as soon as QUORUM permits have been released.
                commit.latch.countDown();

                exchange.sendResponseHeaders(200, -1);
            } finally {
                exchange.close();
            }
        });

        this.ackServer.start();

        // Record the actual bound address (important when ackPort == 0).
        this.ackAddress = new InetSocketAddress(
                resolveLocalHostname(),
                this.ackServer.getAddress().getPort());

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
     * @param partition the partition number — used as the Kafka topic.
     * @param bucket    object bucket.
     * @param key       object key.
     * @return {@code true} iff at least {@value #QUORUM} SNs ACKed within the timeout.
     */
    public boolean beginCommit(UUID requestId, int partition, String bucket, String key) {
        return runCommit(requestId, () -> {
            FileCommitMessage msg = new FileCommitMessage(
                    bucket, key, requestId.toString(), ackAddress);
            pubSubClient.pub(topicFor(partition), Message.serializeMessage(msg));
            logger.debug("Published FileCommitMessage for requestId {} on topic {}",
                    requestId, topicFor(partition));
        });
    }

    /**
     * Publishes a multipart commit command and blocks until quorum.
     *
     * @param requestId the UUID for the upload.
     * @param partition the partition number — used as the Kafka topic.
     * @param bucket    object bucket.
     * @param key       object key.
     * @param chunks    ordered list of part/chunk identifiers.
     * @return {@code true} iff at least {@value #QUORUM} SNs ACKed within the timeout.
     */
    public boolean beginMultipartCommit(UUID requestId, int partition, String bucket, String key, List<String> chunks) {
        return runCommit(requestId, () -> {
            MultipartCommitMessage msg = new MultipartCommitMessage(
                    bucket, key, requestId.toString(), ackAddress, chunks);
            pubSubClient.pub(topicFor(partition), Message.serializeMessage(msg));
            logger.debug("Published MultipartCommitMessage for requestId {} on topic {}",
                    requestId, topicFor(partition));
        });
    }

    @Override
    public void close() {
        // Give any in-flight HTTP exchanges a moment to drain, then stop.
        ackServer.stop(1);
        logger.info("CommitCoordinator ACK server stopped");
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Core template used by both {@link #beginCommit} and {@link #beginMultipartCommit}.
     *
     * <ol>
     *   <li>Registers the pending commit (so arriving ACKs can find it).</li>
     *   <li>Runs {@code publishAction} to send the Kafka/pubsub message.</li>
     *   <li>Waits up to {@value #ACK_TIMEOUT_SECONDS}s for {@value #QUORUM} ACKs.</li>
     *   <li>Cleans up the in-flight entry regardless of outcome.</li>
     * </ol>
     */
    private boolean runCommit(UUID requestId, ThrowingRunnable publishAction) {
        String id = requestId.toString();

        // A CountDownLatch initialised to QUORUM: each ACK calls countDown().
        // The coordinator thread unblocks exactly when the QUORUM-th ACK arrives.
        // Extra ACKs from the remaining SNs call countDown() on an already-zero
        // latch, which is a safe no-op.
        PendingCommit commit = new PendingCommit(QUORUM);

        // Register BEFORE publishing so there is no window where an ACK could
        // arrive before the entry exists in inFlight.
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
            // Always remove from inFlight so late ACKs get 404 rather than
            // being mistakenly associated with a future request that reuses
            // the same UUID (which shouldn't happen, but defence-in-depth).
            inFlight.remove(id);
        }
    }

    /**
     * Returns a hostname that Storage Nodes can actually connect back to.
     * Prefers an externally visible address; falls back to loopback in dev/test.
     */
    private static String resolveLocalHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
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