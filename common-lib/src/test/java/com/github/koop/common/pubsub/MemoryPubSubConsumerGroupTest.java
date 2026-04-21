package com.github.koop.common.pubsub;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemoryPubSubConsumerGroupTest {

    private MemoryPubSub pubSub;

    @BeforeEach
    void setUp() {
        pubSub = new MemoryPubSub();
        pubSub.start((topic, offset, message) -> {
            // no-op listener for tests that don't need live delivery
        });
    }

    @Test
    void testConsumerGroupResumesFromLastOffset() {
        String topic = "test-topic";
        String groupId = "group-1";

        // Subscribe with consumer group, publish some messages
        pubSub.sub(topic, groupId);
        pubSub.pub(topic, "msg-0".getBytes());
        pubSub.pub(topic, "msg-1".getBytes());
        pubSub.pub(topic, "msg-2".getBytes());

        // Drop subscription (simulates going offline)
        pubSub.drop(topic);

        // Publish more messages while offline
        pubSub.pub(topic, "msg-3".getBytes());
        pubSub.pub(topic, "msg-4".getBytes());

        // Rejoin with the same consumer group ID
        pubSub.sub(topic, groupId);

        // pollBacklog should only return messages missed while offline (msg-3, msg-4)
        List<byte[]> backlog = pubSub.pollBacklog(topic);
        assertEquals(2, backlog.size());
        assertEquals("msg-3", new String(backlog.get(0)));
        assertEquals("msg-4", new String(backlog.get(1)));
    }

    @Test
    void testPollBacklogWithNoBacklog() {
        String topic = "test-topic";
        String groupId = "group-1";

        pubSub.sub(topic, groupId);
        pubSub.pub(topic, "msg-0".getBytes());
        pubSub.pub(topic, "msg-1".getBytes());

        // All messages were consumed live, so no backlog
        List<byte[]> backlog = pubSub.pollBacklog(topic);
        assertTrue(backlog.isEmpty(), "No backlog when all messages were consumed live");
    }

    @Test
    void testPollBacklogEmptyTopic() {
        String topic = "empty-topic";
        String groupId = "group-1";

        pubSub.sub(topic, groupId);
        List<byte[]> backlog = pubSub.pollBacklog(topic);
        assertTrue(backlog.isEmpty());
    }

    @Test
    void testPollBacklogWithoutConsumerGroup() {
        String topic = "no-group-topic";

        pubSub.sub(topic);
        pubSub.pub(topic, "msg-0".getBytes());
        pubSub.pub(topic, "msg-1".getBytes());

        // Without consumer group, pollBacklog returns everything
        List<byte[]> backlog = pubSub.pollBacklog(topic);
        assertEquals(2, backlog.size());
    }

    @Test
    void testPollBacklogAdvancesOffset() {
        String topic = "advance-topic";
        String groupId = "group-1";

        pubSub.sub(topic, groupId);

        // Drop and publish while offline
        pubSub.drop(topic);
        pubSub.pub(topic, "msg-0".getBytes());
        pubSub.pub(topic, "msg-1".getBytes());

        // Rejoin
        pubSub.sub(topic, groupId);

        // First poll gets the backlog
        List<byte[]> backlog1 = pubSub.pollBacklog(topic);
        assertEquals(2, backlog1.size());

        // Second poll should return empty (offset was advanced)
        List<byte[]> backlog2 = pubSub.pollBacklog(topic);
        assertTrue(backlog2.isEmpty(), "Offset should have been advanced past the backlog");
    }

    @Test
    void testMultipleConsumerGroupsIndependent() {
        // This tests that different consumer group IDs maintain separate offsets
        // when they subscribe to the same topic sequentially (not concurrently).
        String topic = "shared-topic";
        String group1 = "group-1";
        String group2 = "group-2";

        // Group 1 subscribes and consumes 2 messages live
        pubSub.sub(topic, group1);
        pubSub.pub(topic, "msg-0".getBytes());
        pubSub.pub(topic, "msg-1".getBytes());
        pubSub.drop(topic);

        // Group 2 subscribes fresh — should see all messages from offset 0
        pubSub.sub(topic, group2);
        List<byte[]> backlog2 = pubSub.pollBacklog(topic);
        assertEquals(2, backlog2.size(), "Group 2 should see all 2 existing messages");
        assertEquals("msg-0", new String(backlog2.get(0)));
        assertEquals("msg-1", new String(backlog2.get(1)));

        // Publish more while group 2 is active
        pubSub.pub(topic, "msg-2".getBytes());
        pubSub.drop(topic);

        // Group 1 rejoins — should see msg-2 (offset was at 2 from live consumption)
        pubSub.sub(topic, group1);
        List<byte[]> backlog1 = pubSub.pollBacklog(topic);
        assertEquals(1, backlog1.size(), "Group 1 should only see msg-2 (missed while offline)");
        assertEquals("msg-2", new String(backlog1.get(0)));
    }

    @Test
    void testMessagesBufferedEvenWithoutSubscribers() {
        String topic = "buffered-topic";

        // Publish without any subscriber
        pubSub.pub(topic, "msg-0".getBytes());
        pubSub.pub(topic, "msg-1".getBytes());

        // Subscribe with consumer group
        pubSub.sub(topic, "group-1");

        // Should see all buffered messages
        List<byte[]> backlog = pubSub.pollBacklog(topic);
        assertEquals(2, backlog.size());
        assertEquals("msg-0", new String(backlog.get(0)));
        assertEquals("msg-1", new String(backlog.get(1)));
    }
}
