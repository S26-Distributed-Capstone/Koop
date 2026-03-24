package com.github.koop.storagenode.db;

public enum Operation {
    PUT,
    DELETE,
    CREATE_BUCKET,
    DELETE_BUCKET;

    public static Operation fromString(String op) {
        switch (op.toUpperCase()) {
            case "PUT":
                return PUT;
            case "DELETE":
                return DELETE;
            case "CREATE_BUCKET":
                return CREATE_BUCKET;
            case "DELETE_BUCKET":
                return DELETE_BUCKET;
            default:
                throw new IllegalArgumentException("Unknown operation: " + op);
        }
    }

    @Override
    public String toString() {
        return this.name();
    }
}
