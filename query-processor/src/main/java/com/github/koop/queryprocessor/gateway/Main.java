package com.github.koop.queryprocessor.gateway;

import com.github.koop.common.pubsub.KafkaPubSub;
import com.github.koop.queryprocessor.processor.cache.RedisCacheClient;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.koop.common.metadata.ErasureSetConfiguration;
import com.github.koop.common.metadata.EtcdFetcher;
import com.github.koop.common.metadata.MetadataClient;
import com.github.koop.common.metadata.PartitionSpreadConfiguration;
import com.github.koop.common.pubsub.MemoryPubSub;
import com.github.koop.common.pubsub.PubSubClient;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageResult;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService.CompletedPart;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService.ObjectSummary;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.processor.CommitCoordinator;
import com.github.koop.queryprocessor.processor.MultipartUploadResult;
import com.github.koop.queryprocessor.processor.StorageWorker;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static Javalin createApp(StorageService storage) {
        var app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.http.maxRequestSize = 100_000_000L;

            config.routes.get("/health", Main::healthHandler);

            // ── Bucket-level routes ─────────────────────────────────────────
            config.routes.put("/{bucket}",    ctx -> createBucketHandler(ctx, storage));
            config.routes.delete("/{bucket}", ctx -> deleteBucketHandler(ctx, storage));
            config.routes.get("/{bucket}",    ctx -> listObjectsHandler(ctx, storage));
            config.routes.head("/{bucket}",   ctx -> headBucketHandler(ctx, storage));

            // ── Object-level routes ─────────────────────────────────────────
            config.routes.get("/{bucket}/<key>", ctx -> getObjectHandler(ctx, storage));

            // ADDED: HeadObject handler for S3 Browser pre-flight checks
            config.routes.head("/{bucket}/<key>", ctx -> headObjectHandler(ctx, storage));

            config.routes.delete("/{bucket}/<key>", ctx -> deleteOrAbortHandler(ctx, storage));
            config.routes.put("/{bucket}/<key>", ctx -> putOrUploadPartHandler(ctx, storage));
            config.routes.post("/{bucket}/<key>", ctx -> postObjectHandler(ctx, storage));
        });

        return app;
    }

    private static boolean verifyContentLength(Context ctx) {
        long contentLength = ctx.contentLength();
        if(contentLength < 0) {
            ctx.status(400);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InvalidRequest",
                    "Content-Length header is required and must be non-negative.", ctx.path()));
            return false;
        }
        return true;
    }

    private static void healthHandler(Context ctx) {
        ctx.result("API Gateway is healthy!");
    }

    private static void createBucketHandler(Context ctx, StorageService storage) {
        // ... (unchanged)
        String bucket = ctx.pathParam("bucket");
        try {
            StorageResult result = storage.createBucket(bucket);
            if (result instanceof StorageResult.Failure f) {
                respondStorageFailure(ctx, f, "/" + bucket);
                return;
            }
            ctx.status(200);
        } catch (Exception e) {
            ctx.status(500);
            ctx.result(buildS3ErrorXml("InternalError", "Internal error.", "/" + bucket));
        }
    }

    private static void deleteBucketHandler(Context ctx, StorageService storage) {
        // ... (unchanged)
        String bucket = ctx.pathParam("bucket");
        try {
            StorageResult result = storage.deleteBucket(bucket);
            if (result instanceof StorageResult.Failure f) {
                respondStorageFailure(ctx, f, "/" + bucket);
                return;
            }
            ctx.status(204);
        } catch (Exception e) {
            ctx.status(500);
            ctx.result(buildS3ErrorXml("InternalError", "Internal error.", "/" + bucket));
        }
    }

    private static void listObjectsHandler(Context ctx, StorageService storage) {
        // ... (unchanged)
        String bucket = ctx.pathParam("bucket");
        String prefix = ctx.queryParam("prefix") != null ? ctx.queryParam("prefix") : "";
        int maxKeys = 1000;
        if (ctx.queryParam("max-keys") != null) {
            try { maxKeys = Integer.parseInt(ctx.queryParam("max-keys")); }
            catch (NumberFormatException ignored) {}
        }
        try {
            List<ObjectSummary> objects = storage.listObjects(bucket, prefix, maxKeys);
            ctx.status(200);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildListObjectsXml(bucket, prefix, objects, maxKeys));
        } catch (Exception e) {
            ctx.status(500);
            ctx.result(buildS3ErrorXml("InternalError", "Internal error.", "/" + bucket));
        }
    }

    private static void headBucketHandler(Context ctx, StorageService storage) {
        // ... (unchanged)
        String bucket = ctx.pathParam("bucket");
        try {
            boolean exists = storage.bucketExists(bucket);
            ctx.status(exists ? 200 : 404);
        } catch (Exception e) {
            ctx.status(500);
        }
    }

    // ─── Object Handlers ─────────────────────────────────────────────────────

    // ADDED: HeadObject implementation
    private static void headObjectHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        try {
            List<ObjectSummary> objects = storage.listObjects(bucket, key, 1);
            var match = objects.stream().filter(o -> o.key().equals(key)).findFirst();

            if (match.isPresent()) {
                ctx.status(200);
                ctx.header("Content-Type", "application/octet-stream");
                ctx.header("Content-Length", String.valueOf(match.get().size()));

                String lastMod = match.get().lastModified();
                ctx.header("Last-Modified", (lastMod == null || lastMod.isEmpty()) ? "1970-01-01T00:00:00.000Z" : lastMod);
                ctx.header("ETag", "\"dummy-etag-12345\"");
            } else {
                ctx.status(404);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in HEAD /" + bucket + "/" + key, e);
            ctx.status(500);
        }
    }

    // UPDATED: Now sets the Content-Length header
    private static void getObjectHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String resourcePath = "/" + bucket + "/" + key;
        try {
            StorageService.GetObjectResult obj = storage.getObject(bucket, key);
            if (obj != null) {
                ctx.status(200);
                ctx.header("Content-Type", "application/octet-stream");

                // INJECT THE CONTENT-LENGTH HEADER
                ctx.header("Content-Length", String.valueOf(obj.size()));
                ctx.header("ETag", "\"dummy-etag-12345\"");
                ctx.result(obj.data());
            } else {
                ctx.status(404);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("NoSuchKey", "The specified key does not exist.", resourcePath));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in GET " + resourcePath, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError", "We encountered an internal error. Please try again.", resourcePath));
        }
    }

    private static void deleteOrAbortHandler(Context ctx, StorageService storage) {
        // ... (unchanged)
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String uploadId = ctx.queryParam("uploadId");
        String resourcePath = "/" + bucket + "/" + key;

        try {
            if (uploadId != null) {
                MultipartUploadResult result = storage.abortMultipartUpload(bucket, key, uploadId);
                if (!result.isSuccess()) {
                    ctx.status(multipartHttpStatus(result.status()));
                    ctx.result(buildS3ErrorXml(multipartS3ErrorCode(result.status()), result.message(), resourcePath));
                    return;
                }
                ctx.status(204);
            } else {
                StorageResult result = storage.deleteObject(bucket, key);
                if (result instanceof StorageResult.Failure f) {
                    respondStorageFailure(ctx, f, resourcePath);
                    return;
                }
                ctx.status(204);
            }
        } catch (Exception e) {
            ctx.status(500);
            ctx.result(buildS3ErrorXml("InternalError", "Internal error.", resourcePath));
        }
    }

    private static void putOrUploadPartHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String uploadId = ctx.queryParam("uploadId");
        String partNumberStr = ctx.queryParam("partNumber");
        String resourcePath = "/" + bucket + "/" + key;
        long contentLength = ctx.contentLength();
        if (!verifyContentLength(ctx)) {
            return;
        }
        try {
            if (uploadId != null && partNumberStr != null) {
                int partNumber = Integer.parseInt(partNumberStr);
                InputStream data = ctx.bodyInputStream();
                MultipartUploadResult result = storage.uploadPart(bucket, key, uploadId, partNumber, data, contentLength);
                if (!result.isSuccess()) {
                    ctx.status(multipartHttpStatus(result.status()));
                    ctx.result(buildS3ErrorXml(multipartS3ErrorCode(result.status()), result.message(), resourcePath));
                    return;
                }
                ctx.status(200);
                ctx.result("");
            } else {
                InputStream data = ctx.bodyInputStream();
                StorageResult result = storage.putObject(bucket, key, data, contentLength);
                if (result instanceof StorageResult.Failure f) {
                    respondStorageFailure(ctx, f, resourcePath);
                    return;
                }
                ctx.status(200);
                ctx.result("");
            }
        } catch (Exception e) {
            ctx.status(500);
            ctx.result(buildS3ErrorXml("InternalError", "Internal error.", resourcePath));
        }
    }

    private static void postObjectHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String resourcePath = "/" + bucket + "/" + key;
        boolean isInitiate = ctx.queryParamMap().containsKey("uploads");
        String uploadId = ctx.queryParam("uploadId");

        try {
            if (isInitiate) {
                String newUploadId = storage.initiateMultipartUpload(bucket, key);
                ctx.status(200);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildInitiateMultipartUploadXml(bucket, key, newUploadId));
            } else if (uploadId != null) {
                List<CompletedPart> parts = parseCompletedPartsXml(ctx.body());
                MultipartUploadResult result = storage.completeMultipartUpload(bucket, key, uploadId, parts);
                if (!result.isSuccess()) {
                    ctx.status(multipartHttpStatus(result.status()));
                    ctx.result(buildS3ErrorXml(multipartS3ErrorCode(result.status()), result.message(), resourcePath));
                    return;
                }
                ctx.status(200);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildCompleteMultipartUploadXml(bucket, key));
            } else {
                ctx.status(400);
                ctx.result(buildS3ErrorXml("InvalidRequest", "POST requires ?uploads or ?uploadId.", resourcePath));
            }
        } catch (Exception e) {
            ctx.status(500);
            ctx.result(buildS3ErrorXml("InternalError", "Internal error.", resourcePath));
        }
    }

    private static void respondStorageFailure(Context ctx, StorageResult.Failure failure, String resourcePath) {
        ctx.status(failure.httpStatus());
        ctx.header("Content-Type", "application/xml");
        ctx.result(buildS3ErrorXml(failure.code(), failure.message(), resourcePath));
    }

    private static int multipartHttpStatus(MultipartUploadResult.Status status) {
        return switch (status) {
            case SUCCESS -> 200;
            case NOT_FOUND -> 404;
            case CONFLICT -> 409;
            case STORAGE_FAILURE -> 500;
        };
    }

    private static String multipartS3ErrorCode(MultipartUploadResult.Status status) {
        return switch (status) {
            case SUCCESS -> "";
            case NOT_FOUND -> "NoSuchUpload";
            case CONFLICT -> "InvalidPart";
            case STORAGE_FAILURE -> "InternalError";
        };
    }

    public static void main(String[] args) {
        var pubSubClient = new PubSubClient(new KafkaPubSub());
        pubSubClient.start();
        var metadataFetcherMap =Map.of(
                ErasureSetConfiguration.class, "erasure_set_configurations",
                PartitionSpreadConfiguration.class, "partition_spread_configurations"
        );
        var metadataClient = new MetadataClient(new EtcdFetcher(metadataFetcherMap));
        metadataClient.start();
        String redisURL = System.getenv("REDIS_URL");
        var cacheClient = new RedisCacheClient(redisURL);

        var commitCoordinator = new CommitCoordinator(pubSubClient,0);
        StorageWorker storageWorker = new StorageWorker(metadataClient, commitCoordinator);
        StorageService storage = new StorageWorkerService(storageWorker, cacheClient);
        createApp(storage).start(8080);
    }

    static String buildS3ErrorXml(String code, String message, String resource) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<Error>\n" +
                "  <Code>" + escapeXml(code) + "</Code>\n" +
                "  <Message>" + escapeXml(message) + "</Message>\n" +
                "  <Resource>" + escapeXml(resource) + "</Resource>\n" +
                "  <RequestId>" + java.util.UUID.randomUUID() + "</RequestId>\n" +
                "</Error>";
    }

    private static String buildListObjectsXml(String bucket, String prefix,
                                              List<ObjectSummary> objects, int maxKeys) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
        sb.append("  <Name>").append(escapeXml(bucket)).append("</Name>\n");
        sb.append("  <Prefix>").append(escapeXml(prefix)).append("</Prefix>\n");
        sb.append("  <KeyCount>").append(objects.size()).append("</KeyCount>\n");
        sb.append("  <MaxKeys>").append(maxKeys).append("</MaxKeys>\n");
        sb.append("  <IsTruncated>false</IsTruncated>\n");
        for (ObjectSummary obj : objects) {
            String lastModified = (obj.lastModified() == null || obj.lastModified().isEmpty())
                    ? "1970-01-01T00:00:00.000Z"
                    : obj.lastModified();
            String randomEtag = java.util.UUID.randomUUID().toString().replace("-", "");
            sb.append("  <Contents>\n");
            sb.append("    <Key>").append(escapeXml(obj.key())).append("</Key>\n");
            sb.append("    <Size>").append(obj.size()).append("</Size>\n");
            sb.append("    <LastModified>").append(lastModified).append("</LastModified>\n");
            sb.append("    <ETag>\"").append(randomEtag).append("\"</ETag>\n");
            sb.append("    <StorageClass>STANDARD</StorageClass>\n");
            sb.append("  </Contents>\n");
        }
        sb.append("</ListBucketResult>");
        return sb.toString();
    }

    private static String buildInitiateMultipartUploadXml(String bucket, String key, String uploadId) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "  <Bucket>" + escapeXml(bucket) + "</Bucket>\n" +
                "  <Key>" + escapeXml(key) + "</Key>\n" +
                "  <UploadId>" + escapeXml(uploadId) + "</UploadId>\n" +
                "</InitiateMultipartUploadResult>";
    }

    private static String buildCompleteMultipartUploadXml(String bucket, String key) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "  <Location>http://localhost:8080/" + escapeXml(bucket) + "/" + escapeXml(key) + "</Location>\n" +
                "  <Bucket>" + escapeXml(bucket) + "</Bucket>\n" +
                "  <Key>" + escapeXml(key) + "</Key>\n" +
                "</CompleteMultipartUploadResult>";
    }

    private static String escapeXml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    private static List<CompletedPart> parseCompletedPartsXml(String body) {
        List<CompletedPart> parts = new ArrayList<>();
        String[] partBlocks = body.split("<Part>");
        for (int i = 1; i < partBlocks.length; i++) {
            String block = partBlocks[i];
            int partNumber = Integer.parseInt(extractXmlTag(block, "PartNumber"));
            parts.add(new CompletedPart(partNumber));
        }
        return parts;
    }

    private static String extractXmlTag(String xml, String tag) {
        String open = "<" + tag + ">";
        String close = "</" + tag + ">";
        int start = xml.indexOf(open);
        int end = xml.indexOf(close);
        if (start == -1 || end == -1) return "";
        return xml.substring(start + open.length(), end).trim();
    }
}