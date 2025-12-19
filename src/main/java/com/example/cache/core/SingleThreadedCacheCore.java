package com.example.cache.core;

import com.example.cache.cluster.IClusterService;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class SingleThreadedCacheCore<K, V> implements IDistributedCache<K, V> {
    // Intentionally used specific implementation type over here, I don't want this to be overridden using DI
    private final CacheQueue<K, V> queue;
    private final CacheMetrics cacheMetrics;
    private final IClusterService<K> clusterService;

    private final Map<K, CacheEntry<V>> storage = new HashMap<>();
    // TODO: Check later of we can replace CacheTask from CacheOperation and make it also DI, will be efficient for metrics
    private final BlockingQueue<CacheTask<K, V>> taskQueue = new LinkedBlockingQueue<>();

    public SingleThreadedCacheCore(String workerThreadName, CacheQueue<K, V> queue, CacheMetrics cacheMetrics,
                                   IClusterService<K> clusterService) {
        this.queue = queue;
        this.cacheMetrics = cacheMetrics;
        this.clusterService = clusterService;

        Thread worker = new Thread(this::runEventLoop, workerThreadName);
        worker.setDaemon(true);
        worker.start();
    }

    private void runEventLoop() {
        log.info("Single-threaded cache core worker started");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                CacheTask<K, V> task = taskQueue.take();
                executeTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error processing cache task", e);
            }
        }
    }

    private boolean isNotOwner(K key) {
        String ownerId = clusterService.findOwnerNode(key);
        boolean isOwner = ownerId.equals(clusterService.getLocalNodeId());

        if (!isOwner) {
            log.warn("[Cluster.Routing:Cache.Node.NotOwner] [Key={}] [Key Owner Node={}] [Local Node={}]",
                    key, ownerId, clusterService.getAllNodeIds());
        }
        return !isOwner;
    }

    private void executeTask(CacheTask<K, V> task) {
        if (isNotOwner(task.key)) {
            // TODO: Implement the proxy request forward to owner node, to be implemented later
            task.future.complete(null);
            return;
        }

        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        switch (task.type) {
            case PUT:
                handlePut(task, currentTimeInSec);
                break;
            case GET:
                handleGet(task, currentTimeInSec);
                break;
            case DELETE:
                handleDelete(task);
                break;
        }
    }

    private void handleDelete(CacheTask<K, V> task) {
        storage.remove(task.key);
        cacheMetrics.incrementRemoves();
        queue.submit(CacheOperation.of(CacheOperationType.DELETE, task.key));
        task.future.complete(null);
    }

    private void handleGet(CacheTask<K, V> task, long currentTimeInSec) {
        CacheEntry<V> entry = storage.get(task.key);
        if (entry == null || entry.isExpired(currentTimeInSec)) {
            if (entry != null) {
                storage.remove(task.key);
                cacheMetrics.incrementTtlExpirations();
                queue.submit(CacheOperation.of(CacheOperationType.DELETE, task.key));
            }
            cacheMetrics.incrementMisses();
            task.future.complete(null);
        } else {
            cacheMetrics.incrementHits();
            queue.submit(CacheOperation.of(CacheOperationType.GET, task.key));
            task.future.complete(entry.getValue());
        }
    }

    private void handlePut(CacheTask<K, V> task, long currentTimeInSec) {
        CacheEntry<V> newEntry = CacheEntry.<V>builder()
                .value(task.value)
                .expirationTime(currentTimeInSec + task.ttl)
                .creationTime(currentTimeInSec)
                .build();
        storage.put(task.key, newEntry);
        cacheMetrics.incrementPuts();
        queue.submit(CacheOperation.of(CacheOperationType.PUT, task.key, newEntry));
        task.future.complete(null);
    }

    @Override
    public CompletableFuture<V> submitGet(K key) {
        CompletableFuture<V> future = new CompletableFuture<>();
        taskQueue.add(new CacheTask<>(CacheOperationType.GET, key, null, 0, future));
        return future;
    }

    @Override
    public CompletableFuture<Void> submitPut(K key, V value, long ttlInSec) {
        CompletableFuture<V> future = new CompletableFuture<>();
        taskQueue.add(new CacheTask<>(CacheOperationType.PUT, key, value, ttlInSec, future));
        return future.thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> submitDelete(K key) {
        CompletableFuture<V> future = new CompletableFuture<>();
        taskQueue.add(new CacheTask<>(CacheOperationType.DELETE, key, null, 0, future));
        return future.thenApply(v -> null);
    }

    @Override
    public int size() {
        return storage.size();
    }

    private record CacheTask<K, V>(
            CacheOperationType type,
            K key,
            V value,
            long ttl,
            CompletableFuture<V> future
    ) {
    }
}
