package com.github.koop.common;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.test.EtcdClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class MetadataClientTest {

    @RegisterExtension
    public static final EtcdClusterExtension cluster = EtcdClusterExtension.builder()
            .withNodes(1)
            .build();

    @Test
    void testMetadataFetchAndWatch() throws Exception {
        // 1. Prepare raw client to seed data into the cluster
        Client rawClient = Client.builder().endpoints(cluster.clientEndpoints()).build();
        String replicaSetKeyStr = "/config/replica_sets";
        String partitionSpreadKeyStr = "/config/partition_spread";
        
        String initialReplicaSetsJson = "{\"replica_sets\": [{\"number\": 1, \"machines\": [{\"ip\": \"10.0.0.1\", \"port\": 8000}]}]}";
        String initialPartitionSpreadJson = "{\"partition_spread\": [{\"erasure_set\": \"es1\", \"partitions\": [1, 2, 3]}]}";
        
        ByteSequence replicaSetKey = ByteSequence.from(replicaSetKeyStr, StandardCharsets.UTF_8);
        ByteSequence partitionSpreadKey = ByteSequence.from(partitionSpreadKeyStr, StandardCharsets.UTF_8);
        
        // Seed initial data
        rawClient.getKVClient().put(replicaSetKey, ByteSequence.from(initialReplicaSetsJson, StandardCharsets.UTF_8)).get();
        rawClient.getKVClient().put(partitionSpreadKey, ByteSequence.from(initialPartitionSpreadJson, StandardCharsets.UTF_8)).get();

        // 2. Initialize the MetadataClient pointing to the test cluster
        // We create a new Client instance for the application to simulate a real scenario
        Client appClient = Client.builder().endpoints(cluster.clientEndpoints()).build();
        
        try (MetadataClient metadataClient = new MetadataClient(appClient, replicaSetKeyStr, partitionSpreadKeyStr)) {
            metadataClient.start();

            // 3. Verify Initial Fetch
            ReplicaSetConfiguration replicaConfig = metadataClient.getReplicaSetConfiguration();
            PartitionSpreadConfiguration partitionConfig = metadataClient.getPartitionSpreadConfiguration();
            
            assertNotNull(replicaConfig, "ReplicaSetConfiguration should not be null after start");
            assertEquals(1, replicaConfig.getReplicaSets().size());
            assertEquals("10.0.0.1", replicaConfig.getReplicaSets().get(0).getMachines().get(0).getIp());
            
            assertNotNull(partitionConfig, "PartitionSpreadConfiguration should not be null after start");
            assertEquals(1, partitionConfig.getPartitionSpread().size());
            assertEquals("es1", partitionConfig.getPartitionSpread().get(0).getErasureSet());

            // 4. Verify Watch functionality
            // Update ReplicaSet in etcd
            String updatedReplicaJson = "{\"replica_sets\": [{\"number\": 99, \"machines\": [{\"ip\": \"192.168.1.1\", \"port\": 9000}]}]}";
            rawClient.getKVClient().put(replicaSetKey, ByteSequence.from(updatedReplicaJson, StandardCharsets.UTF_8)).get();
            
            // Wait for watch trigger (polling)
            boolean updatedReplica = false;
            for (int i = 0; i < 20; i++) {
                ReplicaSetConfiguration m = metadataClient.getReplicaSetConfiguration();
                if (m != null && !m.getReplicaSets().isEmpty() && m.getReplicaSets().get(0).getNumber() == 99) {
                    updatedReplica = true;
                    assertEquals("192.168.1.1", m.getReplicaSets().get(0).getMachines().get(0).getIp());
                    break;
                }
                Thread.sleep(100);
            }
            assertTrue(updatedReplica, "MetadataClient should have updated ReplicaSetConfiguration via Watch");

            // Update PartitionSpread in etcd
            String updatedPartitionJson = "{\"partition_spread\": []}";
            rawClient.getKVClient().put(partitionSpreadKey, ByteSequence.from(updatedPartitionJson, StandardCharsets.UTF_8)).get();
            
            // Wait for watch trigger (polling)
            boolean updatedPartition = false;
            for (int i = 0; i < 20; i++) {
                PartitionSpreadConfiguration p = metadataClient.getPartitionSpreadConfiguration();
                // We check if it is empty list
                if (p != null && p.getPartitionSpread() != null && p.getPartitionSpread().isEmpty()) {
                    updatedPartition = true;
                    break;
                }
                Thread.sleep(100);
            }
            assertTrue(updatedPartition, "MetadataClient should have updated PartitionSpreadConfiguration via Watch");
        }
        
        rawClient.close();
    }
}