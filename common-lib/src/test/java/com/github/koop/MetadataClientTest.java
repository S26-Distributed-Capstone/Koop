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
}