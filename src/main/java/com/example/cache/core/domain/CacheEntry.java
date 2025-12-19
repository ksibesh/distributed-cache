package com.example.cache.core.domain;

import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

@Builder
@Getter
public class CacheEntry<V> {

    final V value;
    final long creationTime;
    final long expirationTime;

    public boolean isExpired(long now) {
        return now >= expirationTime;
    }
}
