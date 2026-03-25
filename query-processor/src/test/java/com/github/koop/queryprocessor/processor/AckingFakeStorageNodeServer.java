package com.github.koop.queryprocessor.processor;

import com.github.koop.common.messages.Message;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.queryprocessor.processor.CommitCoordinator;
import com.github.koop.queryprocessor.processor.FakeStorageNodeServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends {@link FakeStorageNodeServer} with commit-protocol awareness.
 *
 * <p>On receiving a Kafka commit message for any partition topic, this node
 * simulates the SN behaviour by POSTing an ACK to the address embedded in
 * the {@link Message.FileCommitMessage}. This closes the loop of the two-phase
 * protocol in tests without needing a real Storage Node implementation.
 *
 * <p>Two independent enable flags are exposed:
 * <ul>
 *   <li>{@link #setEnabled} — disables both upload handling AND commit ACKing
 *       (simulates a fully dead node).</li>
 *   <li>{@link #setUploadEnabled} — disables only upload handling; the node
 *       still ACKs commits (simulates a node that missed the stream but
 *       reconstructed its shard from peers).</li>
 * </ul>
 */
public final class AckingFakeStorageNodeServer extends FakeStorageNodeServer {

    private final HttpClient http = HttpClient.newHttpClient();
    private volatile boolean enabled = true;
    private volatile boolean uploadEnabled = true;
    private final AtomicInteger acksSent = new AtomicInteger(0);

    AckingFakeStorageNodeServer(PubSubClient pubSubClient) {
        super();
        // Subscribe to all 99 partition topics so this node receives every
        // commit message regardless of which partition the key maps to.
        for (int p = 0; p < 99; p++) {
            String topic = CommitCoordinator.topicFor(p);
            pubSubClient.sub(topic, (t, offset, bytes) -> {
                if (!enabled) return;
                Message msg = Message.deserializeMessage(bytes);
                if (msg instanceof Message.FileCommitMessage fcm) {
                    sendAck(fcm.requestID(), fcm.sender());
                }
            });
        }
    }

    // Override setEnabled so that disabling a node also prevents ACKs.
    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        super.setEnabled(enabled);
    }

    /** Disables only the shard-upload endpoint; ACKs are still sent. */
    void setUploadEnabled(boolean uploadEnabled) {
        this.uploadEnabled = uploadEnabled;
        // Reflect upload-only disabling on the parent's flag as well, since
        // FakeStorageNodeServer uses a single flag for all endpoints.
        // We override handlePut behaviour by controlling the parent flag here
        // and restoring the overall enabled state.
        super.setEnabled(uploadEnabled);
    }

    void reset() {
        this.enabled = true;
        this.uploadEnabled = true;
        super.setEnabled(true);
        acksSent.set(0);
    }

    int acksSent() {
        return acksSent.get();
    }

    private void sendAck(String requestId, InetSocketAddress coordinator) {
        try {
            URI uri = URI.create(String.format("http://%s:%d/ack/%s",
                    coordinator.getHostString(), coordinator.getPort(), requestId));
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            http.send(req, HttpResponse.BodyHandlers.discarding());
            acksSent.incrementAndGet();
        } catch (Exception e) {
            // In tests a coordinator may have shut down before the last ACK;
            // log and swallow so test teardown doesn't mask real failures.
            System.err.println("AckingFakeStorageNodeServer: ACK failed for "
                    + requestId + ": " + e.getMessage());
        }
    }
}