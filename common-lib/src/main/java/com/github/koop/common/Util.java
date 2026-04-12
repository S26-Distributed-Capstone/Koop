package com.github.koop.common;

public class Util {
    public static String buildObjectKey(String bucket, String object) {
        return bucket + "/" + object;
    }
}
