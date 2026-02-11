package com.github.koop.queryprocessor.gateway.StorageServices;

import java.io.InputStream;
import java.util.UUID;

import com.github.koop.queryprocessor.processor.StorageWorker;

public class StorageWorkerService implements StorageService {
    // This class will act as a bridge between the API Gateway and the StorageWorker.
    // It will implement the StorageService interface and delegate calls to the StorageWorker. 

    //multiple instance of StorageWoker for one instance of StorageWorkerService?

    public StorageWorkerService(){
         //null for now bc i dont know what to put here yet
    }

    @Override
    public void putObject(String bucket, String key, InputStream data) throws Exception {

    }

    @Override
    public InputStream getObject(String bucket, String key) throws Exception {
        return null;
    }

    @Override
    public void deleteObject(String bucket, String key) throws Exception {
        
    }
    
}
