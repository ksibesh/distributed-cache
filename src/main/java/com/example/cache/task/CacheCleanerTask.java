package com.example.cache.task;

import com.example.cache.core.IDistributedCache;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.IEvictionStrategy;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CacheCleanerTask<K, V> implements Runnable {
    private final CacheQueue<K, V> cacheQueue;
    private final TtlQueue<K> ttlQueue;
    private final IEvictionStrategy<K> evictionStrategy;
    private final int maximumSize;
    private final CacheMetrics cacheMetrics;
    private final IDistributedCache<K, V> cacheCore;

    private volatile boolean running = true;

    public CacheCleanerTask(CacheQueue<K, V> cacheQueue, TtlQueue<K> ttlQueue, IEvictionStrategy<K> evictionStrategy,
                            int maximumCacheSize, CacheMetrics cacheMetrics, IDistributedCache<K, V> cacheCore) {

        this.cacheQueue = cacheQueue;
        this.ttlQueue = ttlQueue;
        this.evictionStrategy = evictionStrategy;
        this.maximumSize = maximumCacheSize;
        this.cacheMetrics = cacheMetrics;
        this.cacheCore = cacheCore;
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

    private void dispatchOperation(CacheOperation<K, V> operation) {
        switch (operation.getType()) {
            case PUT:
                evictionStrategy.onPut(operation.getKey());
                log.debug("[CacheCleanerTask.Dispatch.PUT] [key={}]", operation.getKey());
                ttlQueue.add(operation.getEntry().getExpirationTime(), operation.getKey());
                break;
            case GET:
                evictionStrategy.onGet(operation.getKey());
                log.debug("[CacheCleanerTask.Dispatch.ACCESS] [key={}]", operation.getKey());
                break;
            case DELETE:
                evictionStrategy.onDelete(operation.getKey());
                log.debug("[CacheCleanerTask.Dispatch.REMOVE] [key={}]", operation.getKey());
                break;
        }
    }

    private void cleanupExpiredKeys() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        while (!ttlQueue.isEmpty() && ttlQueue.peek().filter(ttl -> ttl <= currentTimeInSec).isPresent()) {
            ttlQueue.poll().stream().flatMap(Set::stream).forEach(key -> {
                cacheCore.submitDelete(key);
                evictionStrategy.onDelete(key);
                cacheMetrics.incrementTtlExpirations();
                log.debug("[CacheCleanerTask.Cleanup.TTL.ExpiredKeys] [key={}]", key);
            });
        }
    }

    private void enforceCapacityLimit() {
        while (cacheCore.size() > maximumSize) {
            Optional<K> keyToEvict = evictionStrategy.evict();
            if (keyToEvict.isPresent()) {
                K key = keyToEvict.get();
                cacheCore.submitDelete(key);
                evictionStrategy.onDelete(key);
                cacheMetrics.incrementEvictions();
                log.debug("[CacheCleanerTask.Eviction] [key={}] [strategy={}]", key, evictionStrategy.getClass().getName());
            } else {
                log.error("[CacheCleanerTask.Eviction.Error] [strategy={}]", evictionStrategy.getClass().getName());
                break;
            }
        }
    }
}
