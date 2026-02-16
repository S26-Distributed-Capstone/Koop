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
        String metadataKey = "/config/metadata";
        String initialJson = "{\"replica_sets\": [{\"number\": 1, \"machines\": [{\"ip\": \"10.0.0.1\", \"port\": 8000}]}], \"partition_spread\": []}";
        
        ByteSequence key = ByteSequence.from(metadataKey, StandardCharsets.UTF_8);
        ByteSequence value = ByteSequence.from(initialJson, StandardCharsets.UTF_8);
        
        // Seed initial data
        rawClient.getKVClient().put(key, value).get();

        // 2. Initialize the MetadataClient pointing to the test cluster
        // We create a new Client instance for the application to simulate a real scenario
        Client appClient = Client.builder().endpoints(cluster.clientEndpoints()).build();
        
        try (MetadataClient metadataClient = new MetadataClient(appClient, metadataKey)) {
            metadataClient.start();

            // 3. Verify Initial Fetch
            Metadata metadata = metadataClient.getMetadata();
            assertNotNull(metadata, "Metadata should not be null after start");
            assertEquals(1, metadata.getReplicaSets().size());
            assertEquals("10.0.0.1", metadata.getReplicaSets().get(0).getMachines().get(0).getIp());

            // 4. Verify Watch functionality
            // Update the data in etcd
            String updatedJson = "{\"replica_sets\": [{\"number\": 99, \"machines\": [{\"ip\": \"192.168.1.1\", \"port\": 9000}]}], \"partition_spread\": []}";
            rawClient.getKVClient().put(key, ByteSequence.from(updatedJson, StandardCharsets.UTF_8)).get();

            // Wait for the watch to trigger update (simple polling)
            boolean updated = false;
            for (int i = 0; i < 20; i++) {
                Metadata m = metadataClient.getMetadata();
                if (m != null && m.getReplicaSets().get(0).getNumber() == 99) {
                    updated = true;
                    assertEquals("192.168.1.1", m.getReplicaSets().get(0).getMachines().get(0).getIp());
                    break;
                }
                Thread.sleep(100);
            }
            
            assertTrue(updated, "MetadataClient should have updated its state via Watch");
        }
        
        rawClient.close();
    }
}