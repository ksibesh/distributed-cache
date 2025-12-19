package com.example.cache.api.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class GetResponse extends BaseResponse {
    String value;
}
