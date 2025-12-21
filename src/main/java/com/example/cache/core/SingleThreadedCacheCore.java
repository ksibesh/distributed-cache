package com.example.cache.core;

import com.example.cache.cluster.IClusterService;
import com.example.cache.cluster.grpc.CacheGrpcClient;
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
public class SingleThreadedCacheCore implements IDistributedCache {
    private final CacheQueue queue;
    private final CacheMetrics cacheMetrics;
    private final IClusterService clusterService;
    private final CacheGrpcClient grpcClient;

    private final Map<String, CacheEntry> storage = new HashMap<>();
    private final BlockingQueue<CacheTask> taskQueue = new LinkedBlockingQueue<>();

    public SingleThreadedCacheCore(String workerThreadName, CacheQueue queue, CacheMetrics cacheMetrics,
                                   IClusterService clusterService, CacheGrpcClient grpcClient) {
        this.queue = queue;
        this.cacheMetrics = cacheMetrics;
        this.clusterService = clusterService;
        this.grpcClient = grpcClient;

        Thread worker = new Thread(this::runEventLoop, workerThreadName);
        worker.setDaemon(true);
        worker.start();
    }

    private void runEventLoop() {
        log.info("Single-threaded cache core worker started");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                CacheTask task = taskQueue.take();
                executeTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error processing cache task", e);
            }
        }
    }

    private boolean isNotOwner(String key) {
        String ownerId = clusterService.findOwnerNode(key);
        boolean isOwner = ownerId.equals(clusterService.getLocalNodeId());

        if (!isOwner) {
            log.warn("[Cluster.Routing:Cache.Node.NotOwner] [Key={}] [Key Owner Node={}] [Local Node={}]",
                    key, ownerId, clusterService.getAllNodeIds());
        }
        return !isOwner;
    }

    private void executeTask(CacheTask task) {
        if (isNotOwner(task.key)) {
            handleForwarding(task);
            return;
        }

        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        switch (task.type) {
            case PUT -> handlePut(task, currentTimeInSec);
            case GET -> handleGet(task, currentTimeInSec);
            case DELETE -> handleDelete(task);
        }
    }

    private void handleForwarding(CacheTask task) {
        String ownerId = clusterService.findOwnerNode(task.key);
        String ownerNodeAddress = clusterService.getAddressForNodeId(ownerId);
        if (ownerNodeAddress == null || ownerNodeAddress.isEmpty()) {
            log.error("[Cluster.Routing:AddressNotFound] [msg=No address found for owner node] [Owner Node={}] [Key={}]",
                    ownerId, task.key);
            task.future.completeExceptionally(new RuntimeException("Address not found for owner node=" + ownerId));
        }

        log.debug("[Cluster.Routing:Forwarding] [msg=Forwarding cache operation] [Owner Node={}] [Target Address={}] [Key={}]",
                ownerId, ownerNodeAddress, task.key);
        switch (task.type) {
            case PUT -> grpcClient.forwardPut("", task.key, task.value(), task.ttl, task.future);
            case GET -> grpcClient.forwardGet("", task.key, task.future);
            case DELETE -> grpcClient.forwardDelete("", task.key, task.future);
        }
    }

    private void handleDelete(CacheTask task) {
        storage.remove(task.key);
        cacheMetrics.incrementRemoves();
        queue.submit(CacheOperation.of(CacheOperationType.DELETE, task.key));
        task.future.complete(null);
    }

    private void handleGet(CacheTask task, long currentTimeInSec) {
        CacheEntry entry = storage.get(task.key);
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

    private void handlePut(CacheTask task, long currentTimeInSec) {
        CacheEntry newEntry = CacheEntry.builder()
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
    public CompletableFuture<String> submitGet(String key) {
        CompletableFuture<String> future = new CompletableFuture<>();
        taskQueue.add(new CacheTask(CacheOperationType.GET, key, null, 0, future));
        return future;
    }

    @Override
    public CompletableFuture<Void> submitPut(String key, String value, long ttlInSec) {
        CompletableFuture<String> future = new CompletableFuture<>();
        taskQueue.add(new CacheTask(CacheOperationType.PUT, key, value, ttlInSec, future));
        return future.thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> submitDelete(String key) {
        CompletableFuture<String> future = new CompletableFuture<>();
        taskQueue.add(new CacheTask(CacheOperationType.DELETE, key, null, 0, future));
        return future.thenApply(v -> null);
    }

    @Override
    public int size() {
        return storage.size();
    }

    private record CacheTask(
            CacheOperationType type,
            String key,
            String value,
            long ttl,
            CompletableFuture<String> future
    ) {
    }
}
