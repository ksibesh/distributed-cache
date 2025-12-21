package com.example.cache.core.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Builder
@ToString
public class CacheOperation {
    private final CacheOperationType type;
    private final String key;
    private final CacheEntry entry;
    private final Instant timestamp;

    public static CacheOperation of(CacheOperationType type, String key) {
        return CacheOperation.builder()
                .type(type)
                .key(key)
                .timestamp(Instant.now())
                .build();
    }

    public static CacheOperation of(CacheOperationType type, String key, CacheEntry entry) {
        return CacheOperation.builder()
                .type(type)
                .key(key)
                .entry(entry)
                .timestamp(Instant.now())
                .build();
    }
}
