package com.github.koop.queryprocessor.processor;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

//hashes and assigns erasure set

public final class ErasureRouting {
    private ErasureRouting() {}

    // hash(key)%100 : 0-33 -> 1, 34-66 -> 2, 67-99 -> 3
    public static int setForKey(String key) {
        CRC32 crc = new CRC32();
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        crc.update(b, 0, b.length);
        int mod = (int) (crc.getValue() % 100);
        if (mod <= 33) return 1;
        if (mod <= 66) return 2;
        return 3;
    }
}