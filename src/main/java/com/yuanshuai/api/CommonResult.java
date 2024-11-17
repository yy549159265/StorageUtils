package com.yuanshuai.api;

import com.yuanshuai.api.ResultCode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommonResult<T> {

    private int code;

    private String message;

    private T data;



    public static <T> CommonResult<T> success(T data) {
        return new CommonResult<>(ResultCode.SUCCESS.getCode(), ResultCode.SUCCESS.getMessage(), data);
    }


    public static <T> CommonResult<T> success(String message,T data) {
        return new CommonResult<>(ResultCode.SUCCESS.getCode(), message, data);
    }



    public static <T> CommonResult<T> failed(T data) {
        return new CommonResult<>(ResultCode.FAILURE.getCode(), ResultCode.FAILURE.getMessage(), data);
    }

    public static <T> CommonResult<T> failed(String message,T data) {
        return new CommonResult<>(ResultCode.FAILURE.getCode(), message, data);
    }
}
