package com.yuanshuai.api;

public enum ResultCode implements IResultCode{
    SUCCESS(200, "成功"),
    FAILURE(400, "失败");


    private int code;
    private String message;


    ResultCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
