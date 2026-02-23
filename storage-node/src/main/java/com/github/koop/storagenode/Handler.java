package com.github.koop.storagenode;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.github.koop.common.messages.MessageReader;

public interface Handler {
    
    void handle(SocketChannel socket, MessageReader messageReader) throws IOException;
}
