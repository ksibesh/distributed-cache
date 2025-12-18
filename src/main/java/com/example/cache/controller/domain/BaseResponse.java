package com.example.cache.controller.domain;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public abstract class BaseResponse {
    private String errorCode;
    private String errorMessage;
}
