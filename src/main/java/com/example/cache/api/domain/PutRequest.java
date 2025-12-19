package com.example.cache.api.domain;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class PutRequest {
    private String key;
    private String value;
    private long ttlInSec;
}
