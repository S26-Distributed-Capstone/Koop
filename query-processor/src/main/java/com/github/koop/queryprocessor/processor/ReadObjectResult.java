package com.github.koop.queryprocessor.processor;

import java.io.InputStream;

/**
 * Result of reading an object from storage.
 */
public sealed interface ReadObjectResult
        permits ReadObjectResult.Found, ReadObjectResult.Missing, ReadObjectResult.Deleted {

    record Found(InputStream data) implements ReadObjectResult {
    }

    record Missing() implements ReadObjectResult {
    }

    record Deleted() implements ReadObjectResult {
    }
}