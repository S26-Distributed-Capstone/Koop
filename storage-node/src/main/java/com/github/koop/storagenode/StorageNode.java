package com.github.koop.storagenode;
import java.nio.file.Path;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;

public class StorageNode {
    private final String dataDirectory;

    public StorageNode(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }


    protected String getDataDirectory() {
        return dataDirectory;
    }

    protected String getPathFor(int partition, String key){
        // partition/key/data.dat
        return Path.of(dataDirectory, "partition_" + partition, key, "data.dat").toString();
    }





}