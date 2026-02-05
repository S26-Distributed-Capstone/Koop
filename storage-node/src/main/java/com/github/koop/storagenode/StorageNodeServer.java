package com.github.koop.storagenode;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.parsetools.RecordParser;

public class StorageNodeServer extends AbstractVerticle {
    private final int port;
    private final StorageNode storageNode;
    private final NetServer server;

    private final OpenOptions openOptions;

    public StorageNodeServer(int port) {
        this.port = port;
        this.storageNode = new StorageNode("data");
        this.openOptions = new OpenOptions();
        this.server = Vertx.vertx().createNetServer();
    }

    public static void main(String[] args){
        var vertx = Vertx.vertx();
        var options = new DeploymentOptions().setThreadingModel(ThreadingModel.VIRTUAL_THREAD).setInstances(Runtime.getRuntime().availableProcessors());
        vertx.deployVerticle(new StorageNodeServer(7000), options);
    }

    @Override
    public void start() {
        server.connectHandler(socket -> {
            try {
                RecordParser parser = RecordParser.newFixed(4, socket);
                Buffer intBuf = awaitBuffer(parser);
                int partitionId = intBuf.getInt(0);

                parser.fixedSizeMode(4);
                Buffer keyLengthBuf = awaitBuffer(parser);
                int keyLength = keyLengthBuf.getInt(0);
                parser.fixedSizeMode(keyLength);
                Buffer keyBuf = awaitBuffer(parser);
                String key = keyBuf.toString();

                socket.pause();
                parser.handler(null);

                AsyncFile file = Future
                        .await(vertx.fileSystem().open(this.storageNode.getPathFor(partitionId, key), openOptions));
                var writeTask = socket.pipeTo(file);
                writeTask.onFailure(throwable -> {
                    System.err.println("Error writing file: " + throwable.getMessage());
                });
                socket.resume();
                Future.await(writeTask);
                socket.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).listen(port, res -> {
            if (res.succeeded()) {
                System.out.println("TCP server started on port " + port);
            } else {
                System.err.println("Failed to start TCP server: " + res.cause());
            }
        });
    }

    private Buffer awaitBuffer(RecordParser parser) {
        Promise<Buffer> promise = Promise.promise();
        parser.handler(promise::complete); // Complete the future when bytes arrive
        parser.exceptionHandler(promise::fail);
        return Future.await(promise.future());
    }

}
