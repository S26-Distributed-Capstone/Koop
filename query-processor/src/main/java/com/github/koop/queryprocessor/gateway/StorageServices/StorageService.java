package com.github.koop.queryprocessor.gateway.StorageServices;

import com.github.koop.queryprocessor.processor.MultipartUploadResult;

import java.io.InputStream;
import java.util.List;

public interface StorageService {

    // ─── Object Operations ────────────────────────────────────────────────────

    // ADDED: Wrapper record to hold the stream and the exact byte size
    record GetObjectResult(InputStream data, long size) {}

    StorageResult putObject(String bucket, String key, InputStream data, long length) throws Exception;

    // UPDATED: Return the wrapper record instead of InputStream
    GetObjectResult getObject(String bucket, String key) throws Exception;

    StorageResult deleteObject(String bucket, String key) throws Exception;

    // ─── Bucket Operations ────────────────────────────────────────────────────

    StorageResult createBucket(String bucket) throws Exception;
    StorageResult deleteBucket(String bucket) throws Exception;

    List<ObjectSummary> listObjects(String bucket, String prefix, int maxKeys) throws Exception;

    boolean bucketExists(String bucket) throws Exception;

    // ─── Multipart Upload Operations ──────────────────────────────────────────

    String initiateMultipartUpload(String bucket, String key) throws Exception;

    MultipartUploadResult uploadPart(String bucket, String key, String uploadId,
                                     int partNumber, InputStream data, long length) throws Exception;

    MultipartUploadResult completeMultipartUpload(String bucket, String key, String uploadId,
                                                  List<CompletedPart> parts) throws Exception;

    MultipartUploadResult abortMultipartUpload(String bucket, String key, String uploadId) throws Exception;

    // ─── Value types ─────────────────────────────────────────────────────────

    record ObjectSummary(String key, long size, String lastModified) {}
    record CompletedPart(int partNumber) {}
}