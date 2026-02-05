package com.github.koop.storagenode;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import java.rmi.server.ObjID;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class StorageNodeTest {

    private String deploymentId;

    @BeforeEach
    void deployStorageWorker(Vertx vertx, VertxTestContext testContext) {
        vertx.deployVerticle(new StorageNodeServer(7000), new DeploymentOptions()
                .setThreadingModel(ThreadingModel.VIRTUAL_THREAD))
                .onSuccess(id -> {
                    this.deploymentId = id; // Save the ID for cleanup
                    testContext.completeNow();
                })
                .onFailure(testContext::failNow);
    }

    @AfterEach
    void cleanup(Vertx vertx, VertxTestContext testContext) {
        if (deploymentId != null) {
            // Undeploying stops the server and releases the TCP port
            vertx.undeploy(deploymentId, testContext.succeedingThenComplete());
        } else {
            testContext.completeNow();
        }
    }

    @Test
    @Disabled("Disabled until virtual thread client issues are resolved")
    void testPutAndGetBad(Vertx vertx, VertxTestContext testContext) {
        Callable<Void> task = () -> {
            Thread.ofVirtual().start(() -> {
                try {
                    NetClient client = vertx.createNetClient();
                    String testKey = "user-123";
                    String testData = "Hello from the virtual thread client!";
                    int partitionId = 1;

                    // --- 1. PERFORM PUT ---
                    NetSocket putSocket = Future.await(client.connect(7000, "localhost"));

                    Buffer putPacket = Buffer.buffer();
                    putPacket.appendInt(1); // OPCODE_PUT
                    putPacket.appendInt(partitionId); // partitionId
                    putPacket.appendInt(testKey.length());
                    putPacket.appendString(testKey);
                    putPacket.appendString(testData); // The rest is the file body

                    Future.await(putSocket.write(putPacket));
                    putSocket.close();

                    // --- 2. PERFORM GET ---
                    NetSocket getSocket = Future.await(client.connect(7000, "localhost"));

                    Buffer getPacket = Buffer.buffer();
                    getPacket.appendInt(2); // OPCODE_GET
                    getPacket.appendInt(partitionId);
                    getPacket.appendInt(testKey.length());
                    getPacket.appendString(testKey);

                    // We create a promise to capture the full body
                    Promise<Buffer> bodyPromise = Promise.promise();
                    Buffer responseCollector = Buffer.buffer();

                    getSocket.handler(responseCollector::appendBuffer);
                    getSocket.endHandler(v -> bodyPromise.complete(responseCollector));
                    getSocket.exceptionHandler(bodyPromise::fail);

                    getSocket.write(getPacket);

                    Buffer resultBody = Future.await(bodyPromise.future());
                    testContext.verify(() -> {
                        Assertions.assertEquals(testData, resultBody.toString());
                    });

                    testContext.completeNow();
                } catch (Exception e) {
                    testContext.failNow(e);
                }
            });
            return null;
        };
        vertx.executeBlocking(task);
    }

    @Test
    void testPutAndGet(Vertx vertx, VertxTestContext testContext) {
        // We deploy an anonymous Verticle to host our Client Logic.
        // This forces Vert.x to run this start() method on a managed Virtual Thread.
        vertx.deployVerticle(new AbstractVerticle() {
            @Override
            public void start() {
                try {
                    // Now we are safe to use Future.await()
                    NetClient client = vertx.createNetClient();
                    String testKey = "user-123";
                    String testData = "Hello from the virtual thread client!";
                    int partitionId = 1;

                    // --- 1. PERFORM PUT ---
                    // connect() returns a Future, await() pauses this virtual thread
                    NetSocket putSocket = Future.await(client.connect(7000, "localhost"));

                    Buffer putPacket = Buffer.buffer()
                            .appendInt(1) // OPCODE_PUT
                            .appendInt(partitionId)
                            .appendInt(testKey.length())
                            .appendString(testKey)
                            .appendString(testData);

                    Future.await(putSocket.write(putPacket));
                    putSocket.close();

                    // --- 2. PERFORM GET ---
                    NetSocket getSocket = Future.await(client.connect(7000, "localhost"));

                    Buffer getPacket = Buffer.buffer()
                            .appendInt(2) // OPCODE_GET
                            .appendInt(partitionId)
                            .appendInt(testKey.length())
                            .appendString(testKey);

                    // Helper promise to capture the incoming data
                    Promise<Buffer> bodyPromise = Promise.promise();
                    Buffer responseCollector = Buffer.buffer();

                    getSocket.handler(responseCollector::appendBuffer);
                    getSocket.endHandler(v -> bodyPromise.complete(responseCollector));
                    getSocket.exceptionHandler(bodyPromise::fail);

                    getSocket.write(getPacket);

                    // Pause here until the server sends everything and closes the socket
                    Buffer resultBody = Future.await(bodyPromise.future());

                    // Verify safely
                    testContext.verify(() -> {
                        Assertions.assertEquals(testData, resultBody.toString());
                    });

                    testContext.completeNow();

                } catch (Throwable e) {
                    testContext.failNow(e);
                }
            }
        }, new DeploymentOptions().setThreadingModel(ThreadingModel.VIRTUAL_THREAD));
    }
}
