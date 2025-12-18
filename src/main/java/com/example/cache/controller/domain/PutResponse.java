package com.example.cache.controller.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class PutResponse extends BaseResponse {
    private boolean putStatus = false;
}
