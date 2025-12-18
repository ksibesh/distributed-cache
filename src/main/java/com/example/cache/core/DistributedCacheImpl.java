package com.example.cache.core;

import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.util.SystemUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DistributedCacheImpl<K, V> implements IDistributedCache<K, V> {
    private final Map<K, CacheEntry<V>> cache;
    private final CacheQueue<K> queue;

    public DistributedCacheImpl(ConcurrentHashMap<K, CacheEntry<V>> cacheMap, CacheQueue<K> queue) {
        this.cache = cacheMap;
        this.queue = queue;
    }

    @Override
    public void put(K key, V value, long ttlSeconds) {
        // validate section
        if (null == key || null == value) {
            throw new IllegalArgumentException("Key or Value cannot be null. " +
                    "Try using 'delete' method if your want to delete the key.");
        }
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long expirationTimeInSec = currentTimeInSec + ttlSeconds;

        CacheEntry<V> entry = CacheEntry.<V>builder()
                .creationTime(currentTimeInSec)
                .expirationTime(expirationTimeInSec)
                .lastAccessTime(new AtomicLong(currentTimeInSec))
                .value(value)
                .build();
        this.cache.put(key, entry);
        this.queue.submit(CacheOperation.of(CacheOperationType.PUT, key));
    }

    @Override
    public V get(K key) {
        if (null == key) {
            throw new IllegalArgumentException("Key cannot be null.");
        }

        CacheEntry<V> entry = this.cache.get(key);
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        // check if the entry is found and not expired
        if (null == entry || currentTimeInSec >= entry.getExpirationTime()) {
            if (null != entry) {
                this.cache.remove(key);
                this.queue.submit(CacheOperation.of(CacheOperationType.REMOVE, key));
            }
            return null;
        }
        entry.getLastAccessTime().set(currentTimeInSec);

        this.queue.submit(CacheOperation.of(CacheOperationType.ACCESS, key));
        return entry.getValue();
    }

    @Override
    public void delete(K key) {
        if (null == key) {
            throw new IllegalArgumentException("Key cannot be null.");
        }
        this.cache.remove(key);
        this.queue.submit(CacheOperation.of(CacheOperationType.REMOVE, key));
    }

    @Override
    public int size() {
        return this.cache.size();
    }
}
