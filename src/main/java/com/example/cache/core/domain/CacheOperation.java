package com.example.cache.core.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Builder
@ToString
public class CacheOperation<K> {
    private final CacheOperationType type;
    private final K key;
    private final Instant timestamp;

    public static <K> CacheOperation<K> of(CacheOperationType type, K key) {
        return CacheOperation.<K>builder()
                .type(type)
                .key(key)
                .timestamp(Instant.now())
                .build();
    }
}
