package com.example.cache.core;

import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.util.SystemUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
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
            log.error("[Invalid Input:Cache.PUT] [key={}] [value={}]", key, value);
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
        log.debug("[Cache:PUT] [key={}] [value={}] [ttlInSec={}]", key, value, expirationTimeInSec);
        this.queue.submit(CacheOperation.of(CacheOperationType.PUT, key));
    }

    @Override
    public V get(K key) {
        if (null == key) {
            log.error("[Invalid Input:Cache.GET] [key={}]", key);
            throw new IllegalArgumentException("Key cannot be null.");
        }

        CacheEntry<V> entry = this.cache.get(key);
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        // check if the entry is found and not expired
        // TODO: <Sibesh Kumar> check if this logic can be simplified and check for expired keys can be performed in cleanup task.
        if (null == entry) {
            log.debug("[Cache.GET] [key={}]", key);
            return null;
        } else if (currentTimeInSec >= entry.getExpirationTime()) {
            this.cache.remove(key);
            log.debug("[TTL Expired:Cache.GET] [key={}]", key);
            this.queue.submit(CacheOperation.of(CacheOperationType.REMOVE, key));
            return null;
        } else {
            entry.getLastAccessTime().set(currentTimeInSec);
            log.debug("[Cache.GET] [key={}]", key);
            this.queue.submit(CacheOperation.of(CacheOperationType.ACCESS, key));
            return entry.getValue();
        }
    }

    @Override
    public void delete(K key) {
        if (null == key) {
            log.error("[Invalid Input:Cache.DELETE] [key={}]", key);
            throw new IllegalArgumentException("Key cannot be null.");
        }
        this.cache.remove(key);
        log.error("[Cache.DELETE] [key={}]", key);
        this.queue.submit(CacheOperation.of(CacheOperationType.REMOVE, key));
    }

    @Override
    public int size() {
        int size = this.cache.size();
        log.debug("[Cache.Size] [size={}]", size);
        return size;
    }
}
