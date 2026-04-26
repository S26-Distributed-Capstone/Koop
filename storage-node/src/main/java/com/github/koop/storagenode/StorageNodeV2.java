package com.github.koop.storagenode;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.koop.storagenode.db.Database;
import com.github.koop.storagenode.db.FileVersion;
import com.github.koop.storagenode.db.Metadata;
import com.github.koop.storagenode.db.MultipartFileVersion;
import com.github.koop.storagenode.db.RegularFileVersion;
import com.github.koop.storagenode.db.TombstoneFileVersion;

/**
 * StorageNodeV2 is an enhanced version of StorageNode that integrates with a
 * Database for metadata management and supports multipart uploads.
 * It provides methods for storing, retrieving, and deleting objects, as well as
 * managing buckets. The storage layout on disk is organized by sharding object
 * files into subdirectories based on the first three characters of their
 * request IDs to improve filesystem performance.
 * 
 * Key features:
 * - store(): Writes data to disk using a robust method that handles partial
 * writes and ensures data integrity. The file path is determined by the request
 * ID.
 * - commit(): Records the metadata for a stored object in the database,
 * associating it with a key, partition, sequence number, and request ID.
 * - delete(): Marks an object as deleted in the database by creating a
 * tombstone entry. The actual file on disk can be cleaned up asynchronously.
 * - retrieve(): Fetches the latest version of an object based on its key. It
 * returns a GetObjectResponse which can be a FileObject (for regular files),
 * MultipartData (for multipart uploads), or Tombstone (if the object has been
 * deleted).
 * - multipartCommit(): Records metadata for multipart uploads, allowing
 * retrieval of individual chunks later.
 * - Bucket management: Methods to create and delete buckets, check bucket
 * existence, and list items within a bucket.
 * 
 * The class uses Java NIO for efficient file operations and includes error
 * handling to ensure that partially written files are cleaned up in case of
 * failures. Logging is integrated for better observability of operations and
 * errors.
 */
public class StorageNodeV2 {
    private final Database db;
    private final Path storageDir;
    private final WriteTracker writeTracker;
    private final static Logger logger = LogManager.getLogger(StorageNodeV2.class);

    public StorageNodeV2(Database db, Path dir) {
        this(db, dir, new WriteTracker());
    }

    public StorageNodeV2(Database db, Path dir, WriteTracker writeTracker) {
        this.db = db;
        this.storageDir = dir;
        this.writeTracker = writeTracker;
    }

    private Path getObjectPath(String id) {
        // Use the first three characters of the ID to create a sharded subdirectory
        // structure.
        // Fallback to "000" if the ID is unexpectedly short.
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be null or blank");
        }
        // Reject obvious path traversal and separator characters.
        if (id.contains("..") || id.contains("/") || id.contains("\\")) {
            throw new IllegalArgumentException("id contains invalid path characters");
        }
        // Use the first three characters of the ID to create a sharded subdirectory
        // structure.
        // Fallback to "000" if the ID is unexpectedly short.
        String prefixDir = (id.length() >= 3) ? id.substring(0, 3) : "000";
        Path base = storageDir.normalize();
        Path objectPath = base.resolve("blobs").resolve(prefixDir).resolve(id).normalize();
        return objectPath;
    }

    /**
     * GetObjectResponse is a sealed interface representing the possible responses
     * from a retrieve operation. It can be one of:
     * - FileObject: Represents a regular file object with its data and version
     * metadata.
     * - MultipartData: Represents metadata for a multipart upload, including the
     * list of chunk versions.
     * - Tombstone: Represents a deleted object, containing the tombstone version
     * metadata.
     */
    static sealed interface GetObjectResponse permits FileObject, MultipartData, Tombstone {
    }

    static record MultipartData(MultipartFileVersion version) implements GetObjectResponse {
    }

    static record FileObject(FileChannel data, FileVersion version) implements AutoCloseable, GetObjectResponse {
        @Override
        public void close() throws Exception {
            data.close();
        }

    }

    static record Tombstone(TombstoneFileVersion version) implements GetObjectResponse {

    }

    /**
     * Stores the given data for the specified ID & partition. The data is
     * written to disk using a robust method that handles partial writes and ensures
     * data integrity. The file path is determined by the request ID, which allows
     * for sharding files into subdirectories based on their prefixes.
     *
     * @param partition the partition number for this store operation
     * @param key the object key (bucket/object) identifying what is being stored
     * @param requestID the unique request ID used to derive the file path
     * @param data the data to write
     * @throws IOException if the file cannot be written or the write intent cannot be recorded
     */
    protected void store(
            int partition,
            String key,
            String requestID,
            ReadableByteChannel data) throws IOException {

        Path path = getObjectPath(requestID);
        Files.createDirectories(path.getParent());

        writeTracker.begin(key);
        try {
            write(path, data);
            try {
                db.registerBlobArrival(key, requestID, System.currentTimeMillis());
            } catch (Exception e) {
                throw new IOException("Failed to record uncommitted write intent for requestID: " + requestID, e);
            }
        } finally {
            writeTracker.end(key);
        }
    }

    /**
     * Commits the metadata for a stored object in the database, associating it with
     * the given key, partition, sequence number, and request ID. This method should
     * be called after successfully storing the data on disk to ensure that the
     * metadata is consistent with the stored file.
     * 
     * @param partition
     * @param key
     * @param requestID
     * @param seqNumber
     * @throws Exception
     */
    protected boolean commit(int partition, String key, String requestID, long seqNumber) throws Exception {
        return db.putItem(key, partition, seqNumber, requestID);
    }

    /**
     * Deletes the object associated with the given key and partition by creating a
     * tombstone entry in the database. The actual file on disk can be cleaned up
     * asynchronously by a background process that scans for tombstone entries and
     * deletes the corresponding files.
     * 
     * @param partition
     * @param key
     * @param seqNumber
     * @throws Exception
     */
    protected void delete(int partition, String key, long seqNumber) throws Exception {
        db.deleteItem(key, partition, seqNumber);
    }

    /**
     * Returns the latest version of the object associated with the given key. If
     * the object has been deleted, it returns a Tombstone response. If the object
     * is a regular file, it returns a FileObject response containing a FileChannel
     * to read the data and its version metadata. If the object is a multipart
     * upload, it returns a MultipartData response containing the multipart version
     * metadata. If no version is found for the key, it returns Optional.empty().
     * 
     * @param key
     * @return
     * @throws Exception
     * @see #retrieve(String, long) for retrieving specific versions or handling
     *      multipart uploads
     */
    protected Optional<GetObjectResponse> retrieve(String key) throws Exception {
        return retrieve(key, -1);
    }

    /**
     * Retrieves the given version of the object associated with the given key. If
     * seqNumber is -1, it retrieves the latest version; otherwise, it retrieves the
     * specific version corresponding to the provided sequence number. The method
     * returns an Optional containing a GetObjectResponse, which can be a FileObject
     * (for regular files), MultipartData (for multipart uploads), or Tombstone (if
     * the object has been deleted). If no version is found for the key, it returns
     * Optional.empty().
     * 
     * @param key
     * @param seqNumber
     * @return
     * @throws Exception
     */
    protected Optional<GetObjectResponse> retrieve(String key, long seqNumber) throws Exception {
        var latestOpt = seqNumber == -1 ? db.getLatestFileVersion(key) : db.getFileVersion(key, seqNumber);
        if (latestOpt.isEmpty()) {
            return Optional.empty();
        }
        var latest = latestOpt.get();
        if (latest instanceof TombstoneFileVersion t) {
            return Optional.of(new Tombstone(t));
        } else if (latest instanceof RegularFileVersion r) {
            Path path = getObjectPath(r.location());
            if (!Files.exists(path)) {
                // File metadata exists but physical file is missing.
                // Repair will be triggered by the commit path, not on GET.
                return Optional.empty();
            }
            return Optional.of(new FileObject(FileChannel.open(path, StandardOpenOption.READ), latest));
        } else if (latest instanceof MultipartFileVersion m) {
            if (m.chunks().isEmpty())
                return Optional.empty();
            return Optional.of(new MultipartData(m));
        }

        return Optional.empty();
    }

    /**
     * Commits the metadata for a multipart upload, associating it with the given
     * key, partition, sequence number, request ID, and list of chunk versions. This
     * method should be called after successfully storing all the chunks on disk to
     * ensure that the multipart metadata is consistent with the stored files.
     * 
     * @param partition
     * @param key
     * @param seqNumber
     * @param chunks
     * @throws Exception
     */
    protected void multipartCommit(int partition, String key, long seqNumber, List<String> chunks)
            throws Exception {
        db.putMultipartItem(key, partition, seqNumber, chunks);
    }

    /**
     * Creates a new bucket with the specified key and partition, associating it
     * with the given sequence number. This method should be called before storing
     * any objects in the bucket to ensure that the bucket metadata is properly
     * initialized in the database.
     * 
     * @param partition
     * @param bucketKey
     * @param seqNumber
     * @throws Exception
     */
    protected void createBucket(int partition, String bucketKey, long seqNumber) throws Exception {
        db.createBucket(bucketKey, partition, seqNumber);
    }

    /**
     * Deletes the bucket associated with the given key and partition by creating a
     * tombstone entry in the database. The actual files on disk can be cleaned up
     * asynchronously by a background process that scans for tombstone entries and
     * deletes the corresponding files. This method should be called when a bucket
     * is no longer needed to ensure that its metadata is properly marked as deleted
     * in the database.
     * 
     * @param partition
     * @param bucketKey
     * @param seqNumber
     * @throws Exception
     */
    protected void deleteBucket(int partition, String bucketKey, long seqNumber) throws Exception {
        db.deleteBucket(bucketKey, partition, seqNumber);
    }

    /**
     * Checks if a bucket with the specified key exists in the given partition. This
     * method queries the database for the existence of the bucket metadata and
     * returns true if the bucket exists and is not marked as deleted, or false
     * otherwise. This can be used to verify that a bucket has been successfully
     * created before attempting to store objects in it.
     * 
     * @param bucketKey
     * @return
     * @throws Exception
     */
    protected boolean bucketExists(String bucketKey) throws Exception {
        return db.bucketExists(bucketKey);
    }

    /**
     * Lists the items in the bucket with the specified prefix. This method queries
     * the database for all items that belong to the bucket identified by the given
     * prefix and returns a Stream of Metadata objects representing each item. This
     * can be used to retrieve a list of all objects stored in a bucket or to filter
     * objects based on certain criteria.
     * 
     * @param bucketPrefix
     * @return
     * @throws Exception
     */
    protected Stream<Metadata> listItemsInBucket(String bucketPrefix) throws Exception {
        return db.listItemsInBucket(bucketPrefix);
    }

   private void write(Path path, ReadableByteChannel data) throws IOException {
        try (FileChannel fc = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            long written = 0;
            // Define a chunk size (e.g., 8MB) to request in each transfer block
            final long CHUNK_SIZE = 8 * 1024 * 1024; 
            
            // A tiny 1-byte buffer exclusively used to test for EOF
            java.nio.ByteBuffer eofBuffer = java.nio.ByteBuffer.allocateDirect(1);

            while (true) {
                long transferred = fc.transferFrom(data, written, CHUNK_SIZE);
                
                if (transferred > 0) {
                    written += transferred;
                } else {
                    // transferFrom returned 0. This means either:
                    // 1. The stream is at EOF.
                    // 2. The source channel has no bytes immediately available.
                    
                    // To definitively check for EOF without dropping data, attempt a 1-byte read.
                    eofBuffer.clear();
                    int readBytes = data.read(eofBuffer);
                    
                    if (readBytes == -1) {
                        break; // EOF confirmed, exit the loop
                    } else if (readBytes > 0) {
                        // The stream wasn't at EOF, so write that 1 byte and continue
                        eofBuffer.flip();
                        while (eofBuffer.hasRemaining()) {
                            written += fc.write(eofBuffer, written);
                        }
                    }
                }
            }
            fc.force(true);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                logger.error("Failed to delete file after write error: {}", path, ex);
            }
            throw e;
        }
    }
}