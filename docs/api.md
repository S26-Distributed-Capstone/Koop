# API Reference

KoopDB exposes a subset of the Amazon S3 API. Authentication is intentionally out of scope for this project; all requests are accepted anonymously and any credentials supplied by an S3 client are ignored. Use the normal S3 API to interact.

> **Note on `ETag`:** ETags are not relevant for KoopDB — the gateway does not generate or return ETag headers, and the XML body of `CompleteMultipartUpload` is parsed for `<PartNumber>` only; any `<ETag>` values supplied by the client are ignored.

## Base URL
The API is available via the Query Processor services, typically exposed on ports `9001-9003` in a default Docker Compose setup.

`http://<host>:9001`

## Supported Operations (Exactly the same as S3 API)

### General System
| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Returns `200 OK` if the Query Processor is healthy. |

### Bucket Operations
| Method | Path | Description | S3 Operation |
| :--- | :--- | :--- | :--- |
| `PUT` | `/{bucket}` | Creates a new bucket. | `CreateBucket` |
| `DELETE` | `/{bucket}` | Deletes an empty bucket. | `DeleteBucket` |
| `GET` | `/{bucket}` | Lists objects in a bucket (v2). Supports `prefix` and `max-keys` query params. | `ListObjectsV2` |
| `HEAD` | `/{bucket}` | Checks if a bucket exists within the system. | `HeadBucket` |

> **Note on `501 NotImplemented`:** Bucket and object handlers will return `501 NotImplemented` (as an S3 XML error) if the bound storage backend reports the operation as unsupported. In the production configuration the backend is `StorageWorkerService`, which implements all listed operations, so `501` is not expected during normal use.

### Object Operations
| Method | Path | Description | S3 Operation |
| :--- | :--- | :--- | :--- |
| `PUT` | `/{bucket}/{key}` | Uploads an object. Max size defaults to 100MB. | `PutObject` |
| `GET` | `/{bucket}/{key}` | Retrieves an object. Returns binary stream. | `GetObject` |
| `DELETE` | `/{bucket}/{key}` | Deletes an object. | `DeleteObject` |

### Multipart Upload Operations
| Method | Path | Queue Parameters | Description | S3 Operation |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/{bucket}/{key}` | `?uploads` | Initiates a new multipart upload. Returns an XML payload with `UploadId`. | `CreateMultipartUpload` |
| `PUT` | `/{bucket}/{key}` | `?partNumber=N&uploadId=X` | Uploads a specific part for an upload ID. | `UploadPart` |
| `POST` | `/{bucket}/{key}` | `?uploadId=X` | Completes a multipart upload. Requires an XML body listing parts (see below). | `CompleteMultipartUpload` |
| `DELETE` | `/{bucket}/{key}` | `?uploadId=X` | Aborts a multipart upload and cleans up temporary parts. | `AbortMultipartUpload` |

#### `CompleteMultipartUpload` body format

The body must be a `CompleteMultipartUpload` XML document. Only `<PartNumber>` is consumed by the gateway; `<ETag>` values are ignored.

```xml
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber></Part>
  <Part><PartNumber>2</PartNumber></Part>
  <!-- ... -->
</CompleteMultipartUpload>
```

#### Required headers

- `Content-Length` is required on `PUT` (both `PutObject` and `UploadPart`). Requests without it are rejected.

## Error Responses
The API returns standard S3 XML error responses when applicable.

**Example Error:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>/my-bucket/missing-file.txt</Resource>
  <RequestId>...</RequestId>
</Error>
```

**Common Error Codes:**
- `NoSuchBucket`: The specified bucket does not exist.
- `NoSuchKey`: The specified key does not exist.
- `EntityTooLarge`: The object exceeded maximum allowed size.
- `InternalError`: Unexpected server-side failure.
- `NotImplemented`: The requested S3 operation is not supported by Koop.
