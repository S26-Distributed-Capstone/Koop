package com.github.koop.queryprocessor.gateway;

import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.koop.queryprocessor.gateway.StorageServices.StorageService;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService.CompletedPart;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageService.ObjectSummary;
import com.github.koop.queryprocessor.gateway.StorageServices.StorageWorkerService;
import com.github.koop.queryprocessor.processor.MultipartUploadResult;
import com.github.koop.queryprocessor.processor.StorageWorker;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    /**
     * Creates and configures the Javalin app with all S3-compatible routes.
     * Accepts a StorageService so tests can inject a mock.
     *
     * ┌─────────────────────────────────────────────────────────────────────┐
     * │  S3 Route Map                                                       │
     * ├─────────┬──────────────────────────────┬────────────────────────────┤
     * │ Method  │ Path / Query                 │ Operation                  │
     * ├─────────┼──────────────────────────────┼────────────────────────────┤
     * │ GET     │ /health                      │ Health check               │
     * ├─────────┼──────────────────────────────┼────────────────────────────┤
     * │ PUT     │ /{bucket}                    │ CreateBucket               │
     * │ DELETE  │ /{bucket}                    │ DeleteBucket               │
     * │ GET     │ /{bucket}                    │ ListObjectsV2              │
     * │ HEAD    │ /{bucket}                    │ HeadBucket                 │
     * ├─────────┼──────────────────────────────┼────────────────────────────┤
     * │ GET     │ /{bucket}/{key}              │ GetObject                  │
     * │ DELETE  │ /{bucket}/{key}              │ DeleteObject               │
     * │         │ /{bucket}/{key}?uploadId=X   │   └─ AbortMultipartUpload  │
     * │ PUT     │ /{bucket}/{key}              │ PutObject                  │
     * │         │ /{bucket}/{key}?partNumber=N │   └─ UploadPart            │
     * │         │   &uploadId=X                │                            │
     * │ POST    │ /{bucket}/{key}?uploads      │ CreateMultipartUpload      │
     * │ POST    │ /{bucket}/{key}?uploadId=X   │ CompleteMultipartUpload    │
     * └─────────┴──────────────────────────────┴────────────────────────────┘
     */
    public static Javalin createApp(StorageService storage) {
        var app = Javalin.create(config -> {
            config.concurrency.useVirtualThreads = true;
            config.http.maxRequestSize = 100_000_000L; // 100 MB

            config.routes.get("/health", Main::healthHandler);

            // ── Bucket-level routes ─────────────────────────────────────────
            config.routes.put("/{bucket}",    ctx -> createBucketHandler(ctx, storage));
            config.routes.delete("/{bucket}", ctx -> deleteBucketHandler(ctx, storage));
            config.routes.get("/{bucket}",    ctx -> listObjectsHandler(ctx, storage));
            config.routes.head("/{bucket}",   ctx -> headBucketHandler(ctx, storage));

            // ── Object-level routes ─────────────────────────────────────────
            // GET — plain object retrieval only (no multipart GET needed at gateway)
            config.routes.get("/{bucket}/{key}", ctx -> getObjectHandler(ctx, storage));

            // DELETE — handles both DeleteObject and AbortMultipartUpload
            config.routes.delete("/{bucket}/{key}", ctx -> deleteOrAbortHandler(ctx, storage));

            // PUT — handles both PutObject and UploadPart
            config.routes.put("/{bucket}/{key}", ctx -> putOrUploadPartHandler(ctx, storage));

            // POST — handles both CreateMultipartUpload (?uploads) and CompleteMultipartUpload (?uploadId=...)
            config.routes.post("/{bucket}/{key}", ctx -> postObjectHandler(ctx, storage));
        });

        return app;
    }

    private static void verifyContentLength(Context ctx) {
        long contentLength = ctx.contentLength();
        if(contentLength <= 0) {
            ctx.status(400);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InvalidRequest",
                    "Content-Length header is required and must be greater than 0.", ctx.path()));
        }
    }

    // ─── Health ───────────────────────────────────────────────────────────────

    private static void healthHandler(Context ctx) {
        ctx.result("API Gateway is healthy!");
    }

    // ─── Bucket Handlers ──────────────────────────────────────────────────────

    private static void createBucketHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        try {
            storage.createBucket(bucket);
            ctx.status(200);
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("NotImplemented",
                    "CreateBucket is not yet implemented.", "/" + bucket));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in PUT /" + bucket, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", "/" + bucket));
        }
    }

    private static void deleteBucketHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        try {
            storage.deleteBucket(bucket);
            ctx.status(204);
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("NotImplemented",
                    "DeleteBucket is not yet implemented.", "/" + bucket));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in DELETE /" + bucket, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", "/" + bucket));
        }
    }

    /**
     * ListObjectsV2 — supports ?prefix and ?max-keys query params.
     * Returns an S3-compatible XML listing.
     */
    private static void listObjectsHandler(Context ctx, StorageService storage) {
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
            ctx.result(buildListObjectsXml(bucket, prefix, objects));
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("NotImplemented",
                    "ListObjects is not yet implemented.", "/" + bucket));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in GET /" + bucket, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", "/" + bucket));
        }
    }

    /**
     * HeadBucket — used by S3 clients to check bucket existence and ownership.
     * Returns 200 if the bucket exists, 404 if not.
     */
    private static void headBucketHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        try {
            boolean exists = storage.bucketExists(bucket);
            ctx.status(exists ? 200 : 404);
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in HEAD /" + bucket, e);
            ctx.status(500);
        }
    }

    // ─── Object Handlers ─────────────────────────────────────────────────────

    private static void getObjectHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String resourcePath = "/" + bucket + "/" + key;
        try {
            InputStream data = storage.getObject(bucket, key);
            if (data != null) {
                ctx.status(200);
                ctx.header("Content-Type", "application/octet-stream");
                ctx.header("ETag", "\"dummy-etag-12345\"");
                ctx.result(data);
            } else {
                ctx.status(404);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("NoSuchKey",
                        "The specified key does not exist.", resourcePath));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in GET " + resourcePath, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", resourcePath));
        }
    }

    /**
     * Handles DELETE /{bucket}/{key}.
     *
     * If {@code ?uploadId=X} is present → AbortMultipartUpload.
     * Otherwise              → DeleteObject.
     */
    private static void deleteOrAbortHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String uploadId = ctx.queryParam("uploadId");
        String resourcePath = "/" + bucket + "/" + key;

        try {
            if (uploadId != null) {
                // ── AbortMultipartUpload ──
                MultipartUploadResult result = storage.abortMultipartUpload(bucket, key, uploadId);
                if (!result.isSuccess()) {
                    ctx.status(multipartHttpStatus(result.status()));
                    ctx.header("Content-Type", "application/xml");
                    ctx.result(buildS3ErrorXml(multipartS3ErrorCode(result.status()), result.message(), resourcePath));
                    return;
                }
                ctx.status(204);
            } else {
                // ── DeleteObject ──
                storage.deleteObject(bucket, key);
                ctx.status(204);
            }
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("NotImplemented",
                    "AbortMultipartUpload is not yet implemented.", resourcePath));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in DELETE " + resourcePath, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", resourcePath));
        }
    }

    /**
     * Handles PUT /{bucket}/{key}.
     *
     * If {@code ?partNumber=N&uploadId=X} are both present → UploadPart.
     * Otherwise                                            → PutObject.
     */
    private static void putOrUploadPartHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String uploadId = ctx.queryParam("uploadId");
        String partNumberStr = ctx.queryParam("partNumber");
        String resourcePath = "/" + bucket + "/" + key;
        long contentLength = ctx.contentLength();
        verifyContentLength(ctx);
        try {
            if (uploadId != null && partNumberStr != null) {
                // ── UploadPart ──
                int partNumber = Integer.parseInt(partNumberStr);
                InputStream data = ctx.bodyInputStream();

                MultipartUploadResult result = storage.uploadPart(bucket, key, uploadId, partNumber, data, contentLength);
                if (!result.isSuccess()) {
                    ctx.status(multipartHttpStatus(result.status()));
                    ctx.header("Content-Type", "application/xml");
                    ctx.result(buildS3ErrorXml(multipartS3ErrorCode(result.status()), result.message(), resourcePath));
                    return;
                }

                ctx.status(200);
                //ctx.header("ETag", "\"" + etag + "\"");
                ctx.result("");
            } else {
                // ── PutObject ──
                InputStream data = ctx.bodyInputStream();

                storage.putObject(bucket, key, data, contentLength);
                ctx.status(200);
                //ctx.header("ETag", "\"dummy-etag-12345\"");
                ctx.result("");
            }
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("NotImplemented",
                    "UploadPart is not yet implemented.", resourcePath));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in PUT " + resourcePath, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", resourcePath));
        }
    }

    /**
     * Handles POST /{bucket}/{key}.
     *
     * {@code ?uploads}    (no value, key just present) → CreateMultipartUpload.
     * {@code ?uploadId=X}                              → CompleteMultipartUpload.
     */
    private static void postObjectHandler(Context ctx, StorageService storage) {
        String bucket = ctx.pathParam("bucket");
        String key = ctx.pathParam("key");
        String resourcePath = "/" + bucket + "/" + key;

        // Javalin returns "" for a valueless query param key, null if absent.
        boolean isInitiate = ctx.queryParamMap().containsKey("uploads");
        String uploadId = ctx.queryParam("uploadId");

        try {
            if (isInitiate) {
                // ── CreateMultipartUpload ──
                String newUploadId = storage.initiateMultipartUpload(bucket, key);
                ctx.status(200);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildInitiateMultipartUploadXml(bucket, key, newUploadId));

            } else if (uploadId != null) {
                // ── CompleteMultipartUpload ──
                // Parse the XML body: <CompleteMultipartUpload><Part><PartNumber>N</PartNumber><ETag>...</ETag></Part>...</CompleteMultipartUpload>
                List<CompletedPart> parts = parseCompletedPartsXml(ctx.body());
                MultipartUploadResult result = storage.completeMultipartUpload(bucket, key, uploadId, parts);
                if (!result.isSuccess()) {
                    ctx.status(multipartHttpStatus(result.status()));
                    ctx.header("Content-Type", "application/xml");
                    ctx.result(buildS3ErrorXml(multipartS3ErrorCode(result.status()), result.message(), resourcePath));
                    return;
                }
                ctx.status(200);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildCompleteMultipartUploadXml(bucket, key));

            } else {
                ctx.status(400);
                ctx.header("Content-Type", "application/xml");
                ctx.result(buildS3ErrorXml("InvalidRequest",
                        "POST requires either ?uploads or ?uploadId query parameter.", resourcePath));
            }
        } catch (UnsupportedOperationException e) {
            ctx.status(501);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("NotImplemented",
                    "Multipart upload is not yet implemented.", resourcePath));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in POST " + resourcePath, e);
            ctx.status(500);
            ctx.header("Content-Type", "application/xml");
            ctx.result(buildS3ErrorXml("InternalError",
                    "We encountered an internal error. Please try again.", resourcePath));
        }
    }

    // ─── Multipart Error Helpers ──────────────────────────────────────────────

    /**
     * Maps a multipart operation status to the appropriate HTTP status code.
     * <pre>
     *   NOT_FOUND       → 404
     *   CONFLICT        → 409
     *   STORAGE_FAILURE → 500
     * </pre>
     */
    private static int multipartHttpStatus(MultipartUploadResult.Status status) {
        return switch (status) {
            case SUCCESS -> 200;
            case NOT_FOUND -> 404;
            case CONFLICT -> 409;
            case STORAGE_FAILURE -> 500;
        };
    }

    /**
     * Maps a multipart operation status to the S3 error code string used in
     * the XML error response body.
     */
    private static String multipartS3ErrorCode(MultipartUploadResult.Status status) {
        return switch (status) {
            case SUCCESS -> "";
            case NOT_FOUND -> "NoSuchUpload";
            case CONFLICT -> "InvalidPart";
            case STORAGE_FAILURE -> "InternalError";
        };
    }

    // ─── Entry Point ─────────────────────────────────────────────────────────

    public static void main(String[] args) {
        StorageWorker storageWorker = new StorageWorker();
        StorageService storage = new StorageWorkerService(storageWorker);
        createApp(storage).start(8080);
    }

    // ─── XML Builders ─────────────────────────────────────────────────────────

    static String buildS3ErrorXml(String code, String message, String resource) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<Error>\n" +
               "  <Code>" + code + "</Code>\n" +
               "  <Message>" + message + "</Message>\n" +
               "  <Resource>" + resource + "</Resource>\n" +
               "  <RequestId>" + java.util.UUID.randomUUID() + "</RequestId>\n" +
               "</Error>";
    }

    private static String buildListObjectsXml(String bucket, String prefix, List<ObjectSummary> objects) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
        sb.append("  <Name>").append(bucket).append("</Name>\n");
        sb.append("  <Prefix>").append(prefix).append("</Prefix>\n");
        sb.append("  <KeyCount>").append(objects.size()).append("</KeyCount>\n");
        sb.append("  <MaxKeys>1000</MaxKeys>\n");
        sb.append("  <IsTruncated>false</IsTruncated>\n");
        for (ObjectSummary obj : objects) {
            sb.append("  <Contents>\n");
            sb.append("    <Key>").append(obj.key()).append("</Key>\n");
            sb.append("    <Size>").append(obj.size()).append("</Size>\n");
            sb.append("    <LastModified>").append(obj.lastModified()).append("</LastModified>\n");
            //sb.append("    <ETag>\"").append(obj.etag()).append("\"</ETag>\n");
            sb.append("  </Contents>\n");
        }
        sb.append("</ListBucketResult>");
        return sb.toString();
    }

    private static String buildInitiateMultipartUploadXml(String bucket, String key, String uploadId) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
               "  <Bucket>" + bucket + "</Bucket>\n" +
               "  <Key>" + key + "</Key>\n" +
               "  <UploadId>" + uploadId + "</UploadId>\n" +
               "</InitiateMultipartUploadResult>";
    }

    private static String buildCompleteMultipartUploadXml(String bucket, String key) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
               "  <Location>http://localhost:8080/" + bucket + "/" + key + "</Location>\n" +
               "  <Bucket>" + bucket + "</Bucket>\n" +
               "  <Key>" + key + "</Key>\n" +
               "  <ETag>\"\"</ETag>\n" +
               "</CompleteMultipartUploadResult>";
    }

    /**
     * Minimal XML parser for the CompleteMultipartUpload request body.
     * Expected shape:
     * <pre>
     *   &lt;CompleteMultipartUpload&gt;
     *     &lt;Part&gt;
     *       &lt;PartNumber&gt;1&lt;/PartNumber&gt;
     *     &lt;/Part&gt;
     *     ...
     *   &lt;/CompleteMultipartUpload&gt;
     * </pre>
     *
     * A proper XML parser (javax.xml / JAXB) can replace this once the feature
     * moves out of stub phase, but this avoids adding a dependency right now.
     */
    private static List<CompletedPart> parseCompletedPartsXml(String body) {
        List<CompletedPart> parts = new ArrayList<>();
        String[] partBlocks = body.split("<Part>");
        for (int i = 1; i < partBlocks.length; i++) { // skip index 0 (pre-first-<Part> text)
            String block = partBlocks[i];
            int partNumber = Integer.parseInt(extractXmlTag(block, "PartNumber"));
            //String etag = extractXmlTag(block, "ETag").replace("\"", "");
            parts.add(new CompletedPart(partNumber));
        }
        return parts;
    }

    /** Extracts the text content of the first occurrence of {@code <tag>...</tag>}. */
    private static String extractXmlTag(String xml, String tag) {
        String open = "<" + tag + ">";
        String close = "</" + tag + ">";
        int start = xml.indexOf(open);
        int end = xml.indexOf(close);
        if (start == -1 || end == -1) return "";
        return xml.substring(start + open.length(), end).trim();
    }
}