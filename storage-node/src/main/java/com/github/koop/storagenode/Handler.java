package com.github.koop.storagenode;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface Handler {
    
    void handle(SocketChannel socket, long length) throws IOException;
}
