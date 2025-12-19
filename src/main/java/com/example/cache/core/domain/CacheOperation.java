package com.example.cache.core.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Builder
@ToString
public class CacheOperation<K, V> {
    private final CacheOperationType type;
    private final K key;
    private final CacheEntry<V> entry;
    private final Instant timestamp;

    public static <K, V> CacheOperation<K, V> of(CacheOperationType type, K key) {
        return CacheOperation.<K, V>builder()
                .type(type)
                .key(key)
                .timestamp(Instant.now())
                .build();
    }

    public static <K, V> CacheOperation<K, V> of(CacheOperationType type, K key, CacheEntry<V> entry) {
        return CacheOperation.<K, V>builder()
                .type(type)
                .key(key)
                .entry(entry)
                .timestamp(Instant.now())
                .build();
    }
}
