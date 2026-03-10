package com.github.koop.storagenode.db;

public enum Operation {
    PUT,
    DELETE,
    GET;

    public static Operation fromString(String op) {
        switch (op.toUpperCase()) {
            case "PUT":
                return PUT;
            case "DELETE":
                return DELETE;
            case "GET":
                return GET;
            default:
                throw new IllegalArgumentException("Unknown operation: " + op);
        }
    }

    @Override
    public String toString() {
        return this.name();
    }
}
