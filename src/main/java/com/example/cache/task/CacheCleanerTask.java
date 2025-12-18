package com.example.cache.task;

import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.IEvictionStrategy;
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

    private volatile boolean running = true;

    public CacheCleanerTask(CacheQueue<K> cacheQueue, TtlQueue<K> ttlQueue, IEvictionStrategy<K> evictionStrategy,
                            ConcurrentHashMap<K, CacheEntry<V>> cacheMap, int maximumCacheSize) {

        this.cacheQueue = cacheQueue;
        this.ttlQueue = ttlQueue;
        this.evictionStrategy = evictionStrategy;
        this.cacheMap = cacheMap;
        this.maximumSize = maximumCacheSize;
    }

    @Override
    public void run() {
        log.info("Cache Cleaner Task Started.");
        while (running) {
            try {
                cacheQueue.poll(100, TimeUnit.MILLISECONDS).ifPresent(this::dispatchOperation);
                /*cleanupExpiredKeys();*/
                cleanupExpiredKeys();
                enforceCapacityLimit();
            } catch (Exception e) {
                log.error("Error processing cache operation. Msg={}", e.getMessage(), e);
            }
        }
        log.info("Cache Cleaner Task Stopped.");
    }

    public void stop() {
        this.running = false;
    }

    private void dispatchOperation(CacheOperation<K> operation) {
        switch (operation.getType()) {
            case PUT:
                evictionStrategy.onPut(operation.getKey());

                CacheEntry<V> entry = cacheMap.get(operation.getKey());
                ttlQueue.add(entry.getExpirationTime(), operation.getKey());

                break;
            case ACCESS:
                evictionStrategy.onAccess(operation.getKey());
                break;
            case REMOVE:
                evictionStrategy.onRemove(operation.getKey());
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
                        log.debug("Cleaned expired key. Key={}", key);
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
                log.debug("Eviction made to enforce cache capacity limit. EvictedKey={}, EvictionStrategy={}",
                        key, evictionStrategy.getClass().getName());

            } else {

                log.error("Unable to find an eligible key to evict while enforcing cache capacity limit. EvictionStrategy={}",
                        evictionStrategy.getClass().getName());
                break;

            }
        }
    }
}
