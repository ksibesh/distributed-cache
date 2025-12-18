package com.example.cache.task;

import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.IEvictionStrategy;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CacheCleanerTask<K, V> implements Runnable {
    private final CacheQueue<K> cacheQueue;
    private final TtlQueue<K> ttlQueue;
    private final IEvictionStrategy<K> evictionStrategy;
    private final ConcurrentHashMap<K, CacheEntry<V>> cacheMap;
    private final int maximumSize;
    private final CacheMetrics cacheMetrics;

    private volatile boolean running = true;

    public CacheCleanerTask(CacheQueue<K> cacheQueue, TtlQueue<K> ttlQueue, IEvictionStrategy<K> evictionStrategy,
                            ConcurrentHashMap<K, CacheEntry<V>> cacheMap, int maximumCacheSize, CacheMetrics cacheMetrics) {

        this.cacheQueue = cacheQueue;
        this.ttlQueue = ttlQueue;
        this.evictionStrategy = evictionStrategy;
        this.cacheMap = cacheMap;
        this.maximumSize = maximumCacheSize;
        this.cacheMetrics = cacheMetrics;
    }

    @Override
    public void run() {
        log.info("[CacheCleanerTask.Start]");
        while (running) {
            try {
                cacheQueue.poll(100, TimeUnit.MILLISECONDS).ifPresent(this::dispatchOperation);
                cleanupExpiredKeys();
                enforceCapacityLimit();
            } catch (Exception e) {
                log.error("[CacheCleanerTask.Error] [ErrorMessage={}]", e.getMessage(), e);
            }
        }
        log.info("[CacheCleanerTask.Stop]");
    }

    public void stop() {
        this.running = false;
    }

    private void dispatchOperation(CacheOperation<K> operation) {
        switch (operation.getType()) {
            case PUT:
                evictionStrategy.onPut(operation.getKey());
                log.debug("[CacheCleanerTask.Dispatch.PUT] [key={}]", operation.getKey());
                ttlQueue.add(cacheMap.get(operation.getKey()).getExpirationTime(), operation.getKey());
                break;
            case ACCESS:
                evictionStrategy.onAccess(operation.getKey());
                log.debug("[CacheCleanerTask.Dispatch.ACCESS] [key={}]", operation.getKey());
                break;
            case REMOVE:
                evictionStrategy.onRemove(operation.getKey());
                log.debug("[CacheCleanerTask.Dispatch.REMOVE] [key={}]", operation.getKey());
                break;
        }
    }

    private void cleanupExpiredKeys() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        while (!ttlQueue.isEmpty() && ttlQueue.peek().filter(ttl -> ttl <= currentTimeInSec).isPresent()) {
            ttlQueue.poll().stream().flatMap(Set::stream)
                    .filter(cacheMap::containsKey).forEach(key -> {
                        cacheMap.remove(key);
                        evictionStrategy.onRemove(key);
                        cacheMetrics.incrementTtlExpirations();
                        log.debug("[CacheCleanerTask.Cleanup.TTL.ExpiredKeys] [key={}]", key);
                    });
        }
    }

    private void enforceCapacityLimit() {
        while (cacheMap.size() > maximumSize) {

            Optional<K> keyToEvict = evictionStrategy.evict();

            if (keyToEvict.isPresent()) {

                K key = keyToEvict.get();
                cacheMap.remove(key);
                evictionStrategy.onRemove(key);
                cacheMetrics.incrementEvictions();
                log.debug("[CacheCleanerTask.Eviction] [key={}] [strategy={}]", key, evictionStrategy.getClass().getName());

            } else {

                log.error("[CacheCleanerTask.Eviction.Error] [strategy={}]", evictionStrategy.getClass().getName());
                break;

            }
        }
    }
}
