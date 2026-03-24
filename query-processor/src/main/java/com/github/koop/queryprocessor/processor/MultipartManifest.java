package com.github.koop.queryprocessor.processor;

import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Manifest sent to storage nodes to track multipart upload completion.
 *
 * <p>Storage nodes use this metadata to understand which parts belong to which
 * key and can assist in reconstruction (e.g., reading parts for assembly on
 * subsequent reads).
 */
public record MultipartManifest(
        String uploadId,
        String bucket,
        String key,
        List<Integer> partNumbers) {

    /**
     * Serializes this manifest to JSON bytes for transmission to storage nodes.
     *
     * <p>Format:
     * <pre>
     *   {
     *     "uploadId": "...",
     *     "bucket": "...",
     *     "key": "...",
     *     "partNumbers": [1, 2, 3, ...]
     *   }
     * </pre>
     */
    public byte[] serialize() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"uploadId\":\"").append(escapeJsonString(uploadId)).append("\",");
        sb.append("\"bucket\":\"").append(escapeJsonString(bucket)).append("\",");
        sb.append("\"key\":\"").append(escapeJsonString(key)).append("\",");
        sb.append("\"partNumbers\":[");
        
        if (!partNumbers.isEmpty()) {
            StringJoiner joiner = new StringJoiner(",");
            for (Integer num : partNumbers) {
                joiner.add(num.toString());
            }
            sb.append(joiner.toString());
        }
        
        sb.append("]");
        sb.append("}");
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Escapes a string for safe inclusion in JSON.
     */
    private static String escapeJsonString(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
