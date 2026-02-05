package com.github.koop.storagenode;


import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

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
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;

public class StorageNodeServer extends AbstractVerticle {
    private final int port;
    private final StorageNode storageNode;
    private NetServer server;
    private final Map<Integer, BiConsumer<NetSocket, RecordParser>> handlers;

    private final OpenOptions openOptions;

    private static final int OPCODE_PUT = 1;
    private static final int OPCODE_GET = 2;

    public StorageNodeServer(int port) {
        this.port = port;
        this.storageNode = new StorageNode("data");
        this.openOptions = new OpenOptions();
        this.handlers = new HashMap<>();
    }

    public static void main(String[] args){
        var vertx = Vertx.vertx();
        var options = new DeploymentOptions().setThreadingModel(ThreadingModel.VIRTUAL_THREAD).setInstances(Runtime.getRuntime().availableProcessors());
        vertx.deployVerticle(new StorageNodeServer(7000), options);
    }

    private void registerHandlers(){
        handlers.put(OPCODE_PUT, (socket, parser) -> {
            try {
                parser.fixedSizeMode(4);
                int partitionId = awaitBuffer(parser).getInt(0);
                int keyLength = awaitBuffer(parser).getInt(0);
                parser.fixedSizeMode(keyLength);
                String key = awaitBuffer(parser).toString();
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
        });
        handlers.put(OPCODE_GET, (socket, parser) -> {
            try {
                parser.fixedSizeMode(4);
                int partitionId = awaitBuffer(parser).getInt(0);
                parser.fixedSizeMode(4);
                int keyLength = awaitBuffer(parser).getInt(0);
                parser.fixedSizeMode(keyLength);
                String key = awaitBuffer(parser).toString();
                socket.pause();
                parser.handler(null);
                AsyncFile file = Future
                        .await(vertx.fileSystem().open(this.storageNode.getPathFor(partitionId, key), openOptions));
                var readTask = file.pipeTo(socket);
                readTask.onFailure(throwable -> {
                    System.err.println("Error reading file: " + throwable.getMessage());
                });
                socket.resume();
                Future.await(readTask);
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void start() {
        this.server = super.vertx.createNetServer();
        this.registerHandlers();
        server.connectHandler(socket -> {
            try {
                //read opcode
                RecordParser parser = RecordParser.newFixed(4, socket);
                int opcode = awaitBuffer(parser).getInt(0);
                var handler = handlers.get(opcode);
                if (handler != null) {
                    handler.accept(socket, parser);
                } else {
                    System.err.println("Unknown opcode: " + opcode);
                    socket.close();
                }
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

    @Override
    public void stop(){
        if(server != null){
            server.close();
        }
    }

    private Buffer awaitBuffer(RecordParser parser) {
        Promise<Buffer> promise = Promise.promise();
        parser.handler(promise::complete); // Complete the future when bytes arrive
        parser.exceptionHandler(promise::fail);
        return Future.await(promise.future());
    }

}
