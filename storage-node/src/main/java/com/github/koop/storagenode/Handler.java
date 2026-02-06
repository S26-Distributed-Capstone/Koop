package com.github.koop.storagenode;

import java.io.IOException;
import java.net.Socket;

public interface Handler {
    
    void handle(Socket socket) throws IOException;
}
