package com.github.koop.common;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MetadataClientTest {

    @Container
    public static GenericContainer<?> etcd = new GenericContainer<>("quay.io/coreos/etcd:v3.5.12")
            .withExposedPorts(2379)
            .withCommand(
                    "/usr/local/bin/etcd",
                    "--advertise-client-urls", "http://0.0.0.0:2379",
                    "--listen-client-urls", "http://0.0.0.0:2379",
                    "--initial-advertise-peer-urls", "http://0.0.0.0:2380",
                    "--listen-peer-urls", "http://0.0.0.0:2380",
                    "--initial-cluster", "default=http://0.0.0.0:2380"
            )
            .waitingFor(Wait.forLogMessage(".*ready to serve client requests.*\\n", 1));

    @Test
    void testMetadataFetchAndWatch() throws Exception {
        String endpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        
        // Define separate keys for the two configurations
        String replicaSetKey = "/config/replica_sets";
        String partitionSpreadKey = "/config/partition_spread";

        String initialReplicaSetJson = "{\"replica_sets\": [{\"number\": 1, \"machines\": [{\"ip\": \"10.0.0.1\", \"port\": 8000}]}]}";
        String initialPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [1, 2, 3], \"erasure_set\": \"A\"}]}";

        try (Client rawClient = Client.builder().endpoints(endpoint).build()) {
            // Seed initial data for both keys
            rawClient.getKVClient().put(
                    ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialReplicaSetJson, StandardCharsets.UTF_8)
            ).get();

            rawClient.getKVClient().put(
                    ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialPartitionSpreadJson, StandardCharsets.UTF_8)
            ).get();

            // Initialize app client with the test container endpoint
            Client appClient = Client.builder().endpoints(endpoint).build();
            
            try (MetadataClient metadataClient = new MetadataClient(appClient, replicaSetKey, partitionSpreadKey)) {
                metadataClient.start();

                // 1. Verify Initial Fetch for both configurations
                ReplicaSetConfiguration rsConfig = metadataClient.getReplicaSetConfiguration();
                assertNotNull(rsConfig);
                assertEquals(1, rsConfig.getReplicaSets().get(0).getNumber());
                assertEquals("10.0.0.1", rsConfig.getReplicaSets().get(0).getMachines().get(0).getIp());

                PartitionSpreadConfiguration psConfig = metadataClient.getPartitionSpreadConfiguration();
                assertNotNull(psConfig);
                assertEquals("A", psConfig.getPartitionSpread().get(0).getErasureSet());

                // 2. Verify Watch Updates
                String updatedReplicaSetJson = "{\"replica_sets\": [{\"number\": 99, \"machines\": [{\"ip\": \"192.168.1.1\", \"port\": 9000}]}]}";
                String updatedPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [4, 5, 6], \"erasure_set\": \"B\"}]}";
                
                rawClient.getKVClient().put(
                        ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                        ByteSequence.from(updatedReplicaSetJson, StandardCharsets.UTF_8)
                ).get();

                rawClient.getKVClient().put(
                        ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                        ByteSequence.from(updatedPartitionSpreadJson, StandardCharsets.UTF_8)
                ).get();

                // Poll for updates (Wait up to 5 seconds)
                boolean replicaUpdated = false;
                boolean partitionUpdated = false;

                for (int i = 0; i < 50; i++) {
                    ReplicaSetConfiguration rsc = metadataClient.getReplicaSetConfiguration();
                    if (rsc != null && rsc.getReplicaSets().get(0).getNumber() == 99) {
                        replicaUpdated = true;
                    }

                    PartitionSpreadConfiguration psc = metadataClient.getPartitionSpreadConfiguration();
                    if (psc != null && "B".equals(psc.getPartitionSpread().get(0).getErasureSet())) {
                        partitionUpdated = true;
                    }

                    if (replicaUpdated && partitionUpdated) break;
                    Thread.sleep(100);
                }

                assertTrue(replicaUpdated, "ReplicaSetConfiguration should have updated via Watch");
                assertTrue(partitionUpdated, "PartitionSpreadConfiguration should have updated via Watch");
            }
        }
    }

    @Test
    void testReplicaSetUpdateListenerFiresOnlyOnReplicaSetChange() throws Exception {
        String endpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        String replicaSetKey = "/config/replica_sets_listener";
        String partitionSpreadKey = "/config/partition_spread_listener";

        String initialReplicaSetJson = "{\"replica_sets\": [{\"number\": 1, \"machines\": [{\"ip\": \"10.0.0.1\", \"port\": 8000}]}]}";
        String initialPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [1, 2, 3], \"erasure_set\": \"A\"}]}";

        try (Client rawClient = Client.builder().endpoints(endpoint).build()) {
            // Seed initial data
            rawClient.getKVClient().put(
                    ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialReplicaSetJson, StandardCharsets.UTF_8)
            ).get();
            rawClient.getKVClient().put(
                    ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialPartitionSpreadJson, StandardCharsets.UTF_8)
            ).get();

            Client appClient = Client.builder().endpoints(endpoint).build();
            try (MetadataClient metadataClient = new MetadataClient(appClient, replicaSetKey, partitionSpreadKey)) {
                metadataClient.start();

                final boolean[] listenerFired = {false};
                final ReplicaSetConfiguration[] prevHolder = {null};
                final ReplicaSetConfiguration[] newHolder = {null};

                metadataClient.addReplicaSetUpdateListener((prev, curr) -> {
                    listenerFired[0] = true;
                    prevHolder[0] = prev;
                    newHolder[0] = curr;
                });

                // Change partitionSpread only, should NOT fire replicaSet listener
                String updatedPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [4, 5, 6], \"erasure_set\": \"B\"}]}";
                rawClient.getKVClient().put(
                        ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                        ByteSequence.from(updatedPartitionSpreadJson, StandardCharsets.UTF_8)
                ).get();

                Thread.sleep(300); // Wait for watch event
                assertFalse(listenerFired[0], "ReplicaSet listener should not fire on partitionSpread change");

                // Now change replicaSet, should fire
                String updatedReplicaSetJson = "{\"replica_sets\": [{\"number\": 2, \"machines\": [{\"ip\": \"10.0.0.2\", \"port\": 8001}]}]}";
                rawClient.getKVClient().put(
                        ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                        ByteSequence.from(updatedReplicaSetJson, StandardCharsets.UTF_8)
                ).get();

                for (int i = 0; i < 20 && !listenerFired[0]; i++) Thread.sleep(100);

                assertTrue(listenerFired[0], "ReplicaSet listener should fire on replicaSet change");
                assertNotNull(prevHolder[0]);
                assertEquals(1, prevHolder[0].getReplicaSets().get(0).getNumber());
                assertNotNull(newHolder[0]);
                assertEquals(2, newHolder[0].getReplicaSets().get(0).getNumber());
            }
        }
    }

    @Test
    void testPartitionSpreadUpdateListenerFiresOnlyOnPartitionSpreadChange() throws Exception {
        String endpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        String replicaSetKey = "/config/replica_sets_listener2";
        String partitionSpreadKey = "/config/partition_spread_listener2";

        String initialReplicaSetJson = "{\"replica_sets\": [{\"number\": 1, \"machines\": [{\"ip\": \"10.0.0.1\", \"port\": 8000}]}]}";
        String initialPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [1, 2, 3], \"erasure_set\": \"A\"}]}";

        try (Client rawClient = Client.builder().endpoints(endpoint).build()) {
            // Seed initial data
            rawClient.getKVClient().put(
                    ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialReplicaSetJson, StandardCharsets.UTF_8)
            ).get();
            rawClient.getKVClient().put(
                    ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialPartitionSpreadJson, StandardCharsets.UTF_8)
            ).get();

            Client appClient = Client.builder().endpoints(endpoint).build();
            try (MetadataClient metadataClient = new MetadataClient(appClient, replicaSetKey, partitionSpreadKey)) {
                metadataClient.start();

                final boolean[] listenerFired = {false};
                final PartitionSpreadConfiguration[] prevHolder = {null};
                final PartitionSpreadConfiguration[] newHolder = {null};

                metadataClient.addPartitionSpreadUpdateListener((prev, curr) -> {
                    listenerFired[0] = true;
                    prevHolder[0] = prev;
                    newHolder[0] = curr;
                });

                // Change replicaSet only, should NOT fire partitionSpread listener
                String updatedReplicaSetJson = "{\"replica_sets\": [{\"number\": 2, \"machines\": [{\"ip\": \"10.0.0.2\", \"port\": 8001}]}]}";
                rawClient.getKVClient().put(
                        ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                        ByteSequence.from(updatedReplicaSetJson, StandardCharsets.UTF_8)
                ).get();

                Thread.sleep(300); // Wait for watch event
                assertFalse(listenerFired[0], "PartitionSpread listener should not fire on replicaSet change");

                // Now change partitionSpread, should fire
                String updatedPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [4, 5, 6], \"erasure_set\": \"B\"}]}";
                rawClient.getKVClient().put(
                        ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                        ByteSequence.from(updatedPartitionSpreadJson, StandardCharsets.UTF_8)
                ).get();

                for (int i = 0; i < 20 && !listenerFired[0]; i++) Thread.sleep(100);

                assertTrue(listenerFired[0], "PartitionSpread listener should fire on partitionSpread change");
                assertNotNull(prevHolder[0]);
                assertEquals("A", prevHolder[0].getPartitionSpread().get(0).getErasureSet());
                assertNotNull(newHolder[0]);
                assertEquals("B", newHolder[0].getPartitionSpread().get(0).getErasureSet());
            }
        }
    }

    @Test
    void testListenersReceiveNullOnDelete() throws Exception {
        String endpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        String replicaSetKey = "/config/replica_sets_delete";
        String partitionSpreadKey = "/config/partition_spread_delete";

        String initialReplicaSetJson = "{\"replica_sets\": [{\"number\": 1, \"machines\": [{\"ip\": \"10.0.0.1\", \"port\": 8000}]}]}";
        String initialPartitionSpreadJson = "{\"partition_spread\": [{\"partitions\": [1, 2, 3], \"erasure_set\": \"A\"}]}";

        try (Client rawClient = Client.builder().endpoints(endpoint).build()) {
            // Seed initial data
            rawClient.getKVClient().put(
                    ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialReplicaSetJson, StandardCharsets.UTF_8)
            ).get();
            rawClient.getKVClient().put(
                    ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8),
                    ByteSequence.from(initialPartitionSpreadJson, StandardCharsets.UTF_8)
            ).get();

            Client appClient = Client.builder().endpoints(endpoint).build();
            try (MetadataClient metadataClient = new MetadataClient(appClient, replicaSetKey, partitionSpreadKey)) {
                metadataClient.start();

                final boolean[] replicaListenerFired = {false};
                final ReplicaSetConfiguration[] prevReplica = {null};
                final ReplicaSetConfiguration[] newReplica = {null};

                final boolean[] partitionListenerFired = {false};
                final PartitionSpreadConfiguration[] prevPartition = {null};
                final PartitionSpreadConfiguration[] newPartition = {null};

                metadataClient.addReplicaSetUpdateListener((prev, curr) -> {
                    replicaListenerFired[0] = true;
                    prevReplica[0] = prev;
                    newReplica[0] = curr;
                });

                metadataClient.addPartitionSpreadUpdateListener((prev, curr) -> {
                    partitionListenerFired[0] = true;
                    prevPartition[0] = prev;
                    newPartition[0] = curr;
                });

                // Delete replicaSet key
                rawClient.getKVClient().delete(ByteSequence.from(replicaSetKey, StandardCharsets.UTF_8)).get();

                for (int i = 0; i < 20 && !replicaListenerFired[0]; i++) Thread.sleep(100);

                assertTrue(replicaListenerFired[0], "ReplicaSet listener should fire on delete");
                assertNotNull(prevReplica[0]);
                assertNull(newReplica[0]);

                // Delete partitionSpread key
                rawClient.getKVClient().delete(ByteSequence.from(partitionSpreadKey, StandardCharsets.UTF_8)).get();

                for (int i = 0; i < 20 && !partitionListenerFired[0]; i++) Thread.sleep(100);

                assertTrue(partitionListenerFired[0], "PartitionSpread listener should fire on delete");
                assertNotNull(prevPartition[0]);
                assertNull(newPartition[0]);
            }
        }
    }
}