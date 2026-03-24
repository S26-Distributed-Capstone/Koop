# API Reference

KoopDB exposes a subset of the Amazon S3 API. All requests are authenticated anonymously for now (no signature verification). Use the normal S3 API to interact

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
| `POST` | `/{bucket}/{key}` | `?uploadId=X` | Completes a multipart upload by assembling all uploaded parts. Requires XML body listing parts. | `CompleteMultipartUpload` |
| `DELETE` | `/{bucket}/{key}` | `?uploadId=X` | Aborts a multipart upload and cleans up temporary parts. | `AbortMultipartUpload` |

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
