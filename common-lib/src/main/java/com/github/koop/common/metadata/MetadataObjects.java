package com.github.koop.common.metadata;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum MetadataObjects {
    PARTITION_SPREAD(PartitionSpreadConfiguration.class),
    REPLICA_SETS(ReplicaSetConfiguration.class);

    private final Class<?> objectClass;
    private final String metadataKey;

    MetadataObjects(Class<?> objectClass) {
        this.objectClass = objectClass;
        String simpleName = objectClass.getSimpleName();
        StringBuilder sb = new StringBuilder();
        // Convert CamelCase to snake_case
        for (int i = 0; i < simpleName.length(); i++) {
            char c = simpleName.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
            sb.append('_');
            }
            sb.append(Character.toLowerCase(c));
        }
        this.metadataKey = sb.toString() + ".json";
    }

    public static Map<Class<?>, String> getMetadataKeyMap() {
        return Arrays.stream(MetadataObjects.values())
                .collect(Collectors.toMap(m -> m.objectClass, m -> m.metadataKey));
    }
}
