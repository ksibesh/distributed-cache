package com.example.cache.core.ds;

import com.example.cache.core.domain.CacheOperation;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CacheQueue<K> {
    private final BlockingQueue<CacheOperation<K>> queue;

    public CacheQueue(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
    }

    /**
     * Submits a new operation in the queue. Used by main cache threads (put/get/remove).
     * Non-blocking if capacity is available.
     *
     * @param operation The operation payload
     * @return True if successful, False if queue is full (dropped operation).
     */
    public boolean submit(CacheOperation<K> operation) {
        boolean submitResult = queue.offer(operation);
        log.debug("[CacheQueue.Submit] [operation={}]", operation);
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
    public Optional<CacheOperation<K>> poll(long timeout, TimeUnit unit) {
        try {
            CacheOperation<K> operation = queue.poll(timeout, unit);
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
