package com.example.cache.core.ds;

import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.metrics.CacheMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class CacheQueueTest {

    private CacheQueue<String, String> cacheQueue;

    @BeforeEach
    public void setup() {
        CacheMetrics cacheMetrics = new CacheMetrics();
        cacheQueue = new CacheQueue<String, String>(100, cacheMetrics);
    }

    @Test
    public void testQueueInsertionSingleOperation() {
        String key = "testKey";
        cacheQueue.submit(CacheOperation.of(CacheOperationType.PUT, key));

        Assertions.assertEquals(1, cacheQueue.size());
    }

    @Test
    public void testQueueInsertionMultipleOperation() {
        String[] keys = new String[]{"putKey", "getKey", "deleteKey"};
        cacheQueue.submit(CacheOperation.of(CacheOperationType.PUT, keys[0]));
        cacheQueue.submit(CacheOperation.of(CacheOperationType.GET, keys[1]));
        cacheQueue.submit(CacheOperation.of(CacheOperationType.DELETE, keys[2]));

        Assertions.assertEquals(3, cacheQueue.size());
    }

    @Test
    public void testQueuePollWithMultipleEntriesUntillEmpty() {
        String[] keys = new String[]{"putKey", "getKey", "deleteKey"};
        cacheQueue.submit(CacheOperation.of(CacheOperationType.PUT, keys[0]));
        cacheQueue.submit(CacheOperation.of(CacheOperationType.GET, keys[1]));
        cacheQueue.submit(CacheOperation.of(CacheOperationType.DELETE, keys[2]));

        {
            Assertions.assertEquals(3, cacheQueue.size());
            Optional<CacheOperation<String, String>> operation = cacheQueue.poll(100, TimeUnit.MILLISECONDS);
            Assertions.assertTrue(operation.isPresent());
            Assertions.assertEquals(CacheOperationType.PUT, operation.get().getType());
            Assertions.assertEquals(keys[0], operation.get().getKey());
            Assertions.assertEquals(2, cacheQueue.size());
        }
        {
            Assertions.assertEquals(2, cacheQueue.size());
            Optional<CacheOperation<String, String>> operation = cacheQueue.poll(100, TimeUnit.MILLISECONDS);
            Assertions.assertTrue(operation.isPresent());
            Assertions.assertEquals(CacheOperationType.GET, operation.get().getType());
            Assertions.assertEquals(keys[1], operation.get().getKey());
            Assertions.assertEquals(1, cacheQueue.size());
        }
        {
            Assertions.assertEquals(1, cacheQueue.size());
            Optional<CacheOperation<String, String>> operation = cacheQueue.poll(100, TimeUnit.MILLISECONDS);
            Assertions.assertTrue(operation.isPresent());
            Assertions.assertEquals(CacheOperationType.DELETE, operation.get().getType());
            Assertions.assertEquals(keys[2], operation.get().getKey());
            Assertions.assertEquals(0, cacheQueue.size());
        }
    }

    @Test
    public void testQueuePollBeyondEmptyWithSingleEntry() {
        String key = "testKey";
        cacheQueue.submit(CacheOperation.of(CacheOperationType.PUT, key));

        {
            Assertions.assertEquals(1, cacheQueue.size());
            Optional<CacheOperation<String, String>> operation = cacheQueue.poll(100, TimeUnit.MILLISECONDS);
            Assertions.assertTrue(operation.isPresent());
            Assertions.assertEquals(CacheOperationType.PUT, operation.get().getType());
            Assertions.assertEquals(key, operation.get().getKey());
            Assertions.assertEquals(0, cacheQueue.size());
        }
        {
            Assertions.assertEquals(0, cacheQueue.size());
            Optional<CacheOperation<String, String>> operation = cacheQueue.poll(100, TimeUnit.MILLISECONDS);
            Assertions.assertTrue(operation.isEmpty());
        }
    }

    @Test
    public void testQueuePollForEmptyQueue() {
        Assertions.assertEquals(0, cacheQueue.size());
        Optional<CacheOperation<String, String>> operation = cacheQueue.poll(100, TimeUnit.MILLISECONDS);
        Assertions.assertTrue(operation.isEmpty());
    }
}
