package com.example.cache.core.ds;

import com.example.cache.core.domain.CacheOperation;
import com.example.cache.metrics.CacheMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CacheQueue {
    private final BlockingQueue<CacheOperation> queue;
    private final CacheMetrics cacheMetrics;

    public CacheQueue(int capacity, CacheMetrics cacheMetrics) {
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.cacheMetrics = cacheMetrics;
    }

    /**
     * Submits a new operation in the queue. Used by main cache threads (put/get/remove).
     * Non-blocking if capacity is available.
     *
     * @param operation The operation payload
     * @return True if successful, False if queue is full (dropped operation).
     */
    public boolean submit(CacheOperation operation) {
        boolean submitResult = queue.offer(operation);
        if (!submitResult) {
            cacheMetrics.incrementDroppedOperations();
            log.debug("[Operation Dropped:CacheQueue.Submit] [operation={}] [msg=Queue is full.]", operation);
        } else {
            log.debug("[CacheQueue.Submit] [operation={}]", operation);
        }
        return submitResult;
    }

    /**
     * Polls the queue for an operation, blocking up to a specified timeout
     * Used by CacheCleanerTask
     *
     * @param timeout The maximum time to wait.
     * @param unit    The time unit.
     * @return An Optional containing the operation, or empty if timeout occurred.
     */
    public Optional<CacheOperation> poll(long timeout, TimeUnit unit) {
        try {
            CacheOperation operation = queue.poll(timeout, unit);
            log.debug("[CacheQueue.Poll] [timeout={}, unit={}]", timeout, unit);
            return Optional.ofNullable(operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    public int size() {
        int size = queue.size();
        log.debug("[CacheQueue.Size] [size={}]", size);
        return size;
    }
}
