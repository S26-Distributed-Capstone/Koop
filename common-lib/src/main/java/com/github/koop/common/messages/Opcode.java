package com.github.koop.common.messages;

public enum Opcode {
    SN_PUT(1),
    SN_GET(2),
    SN_DELETE(3);

    private final int code;

    Opcode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
