package com.example.cache.core;

import com.example.cache.cluster.IClusterService;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DistributedCacheImpl<K, V> implements IDistributedCache<K, V> {
    private final Map<K, CacheEntry<V>> cache;
    private final CacheQueue<K> queue;
    private final CacheMetrics cacheMetrics;
    private final IClusterService<K> clusterService;

    public DistributedCacheImpl(ConcurrentHashMap<K, CacheEntry<V>> cacheMap, CacheQueue<K> queue,
                                CacheMetrics cacheMetrics, IClusterService<K> clusterService) {
        this.cache = cacheMap;
        this.queue = queue;
        this.cacheMetrics = cacheMetrics;
        this.clusterService = clusterService;

        log.info("[Cache.Initialized] [Node={}]", clusterService.getLocalNodeId());
    }

    private boolean isOwnerNode(K key) {
        String ownerId = clusterService.findOwnerNode(key);
        boolean isOwner = ownerId.equals(clusterService.getLocalNodeId());

        if (!isOwner) {
            log.warn("[Cluster.Routing:Cache.Node.NotOwner] [Key={}] [Key Owner Node={}] [Local Node={}]",
                    key, ownerId, clusterService.getAllNodeIds());
        }
        return isOwner;
    }

    @Override
    public void put(K key, V value, long ttlSeconds) {
        // validate section
        if (null == key || null == value) {
            log.error("[Invalid Input:Cache.PUT] [key={}] [value={}]", key, value);
            throw new IllegalArgumentException("Key or Value cannot be null. " +
                    "Try using 'delete' method if your want to delete the key.");
        }

        if (!isOwnerNode(key)) {
            return;
        }

        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long expirationTimeInSec = currentTimeInSec + ttlSeconds;

        CacheEntry<V> entry = CacheEntry.<V>builder()
                .creationTime(currentTimeInSec)
                .expirationTime(expirationTimeInSec)
                .value(value)
                .build();
        this.cache.put(key, entry);
        this.cacheMetrics.incrementPuts();
        log.debug("[Cache:PUT] [key={}] [value={}] [ttlInSec={}]", key, value, expirationTimeInSec);
        this.queue.submit(CacheOperation.of(CacheOperationType.PUT, key));
    }

    @Override
    public V get(K key) {
        if (null == key) {
            log.error("[Invalid Input:Cache.GET] [key={}]", key);
            throw new IllegalArgumentException("Key cannot be null.");
        }

        if (!isOwnerNode(key)) {
            return null;
        }

        CacheEntry<V> entry = this.cache.get(key);
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        // check if the entry is found and not expired
        // TODO: <Sibesh Kumar> check if this logic can be simplified and check for expired keys can be performed in cleanup task.
        if (null == entry) {
            this.cacheMetrics.incrementMisses();
            log.debug("[Cache.GET] [key={}]", key);
            return null;
        } else if (currentTimeInSec >= entry.getExpirationTime()) {
            this.cacheMetrics.incrementMisses();
            this.cacheMetrics.incrementTtlExpirations();
            this.cache.remove(key);
            log.debug("[TTL Expired:Cache.GET] [key={}]", key);
            this.queue.submit(CacheOperation.of(CacheOperationType.REMOVE, key));
            return null;
        } else {
            this.cacheMetrics.incrementHits();
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

        if (!isOwnerNode(key)) {
            return;
        }

        if (this.cache.containsKey(key)) {
            this.cacheMetrics.incrementRemoves();
        }
        this.cache.remove(key);
        log.error("[Cache.DELETE] [key={}]", key);
        this.queue.submit(CacheOperation.of(CacheOperationType.REMOVE, key));
    }
}
