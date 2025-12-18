package com.example.cache.core;

import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.metrics.CacheMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

public class DistributedCacheImplTest {

    private IDistributedCache<String, String> cache;

    @BeforeEach
    public void setup() {
        ConcurrentHashMap<String, CacheEntry<String>> cacheMap = new ConcurrentHashMap<>();
        CacheMetrics cacheMetrics = new CacheMetrics();
        CacheQueue<String> queue = new CacheQueue<>(10, cacheMetrics);
        cache = new DistributedCacheImpl<>(cacheMap, queue, cacheMetrics);
    }

    @Test
    public void testPut() {
        String key = "test_key";
        String value = "test_value";
        long ttlInSec = 60;

        cache.put(key, value, ttlInSec);
        Assertions.assertEquals(1, cache.size());
        Assertions.assertEquals(value, cache.get(key));
        Assertions.assertNull(cache.get("random_key"));
    }

    @Test
    public void testPutWithNullKey() {
        String key = null;
        String value = "test_value";
        long ttlInSec = 60;

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            cache.put(key, value, ttlInSec);
        });
    }

    @Test
    public void testPutWithNullValue() {
        String key = "test_key";
        String value = null;
        long ttlInSec = 60;

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            cache.put(key, value, ttlInSec);
        });
    }

    @Test
    public void testGet() {
        String key = "test_key";
        String value = "test_value";
        long ttlInSec = 60;

        cache.put(key, value, ttlInSec);
        String returnedValue = cache.get(key);

        Assertions.assertEquals(value, returnedValue);
    }

    @Test
    public void testGetAfterTtl() throws InterruptedException {
        String key = "test_key";
        String value = "test_value";
        long ttlInSec = 2;

        cache.put(key, value, ttlInSec);
        Thread.sleep(ttlInSec * 1000);

        String returnedValue = cache.get(key);
        Assertions.assertNull(returnedValue);
        Assertions.assertEquals(0, cache.size());
    }

    @Test
    public void testGetWithNullKey() {
        String key = null;
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            cache.get(key);
        });
    }

    @Test
    public void testDelete() {
        String key = "test_key";
        String value = "test_value";
        long ttlInSec = 2;

        cache.put(key, value, ttlInSec);
        Assertions.assertEquals(1, cache.size());

        cache.delete(key);
        Assertions.assertEquals(0, cache.size());
    }

    @Test
    public void testDeleteWithNullKey() {
        String key = null;

        Assertions.assertThrows(IllegalArgumentException.class, () -> cache.delete(key));
    }

    /**
     * Delete return success response when deleting non-existing keys. The caller doesn't need to be aware
     * with the complexity of the system. The intention is to delete if found, and no operation if key not
     * found. To this we are inserting a key and value in cache, validate the size of cache, then try to
     * delete invalid key and re-validate the size of cache, the size shouldn't have changed and method
     * resource should not be exception.
     */
    @Test
    public void testDeleteWithNonExistingKey() {
        String key = "test_key";
        String value = "test_value";
        long ttlInSec = 2;

        cache.put(key, value, ttlInSec);
        Assertions.assertEquals(1, cache.size());

        String nonExistingKey = "test_key_v2";
        cache.delete(nonExistingKey);
        Assertions.assertEquals(1, cache.size());
    }

    @Test
    public void testSize() {
        Assertions.assertEquals(0, cache.size());

        String key = "test_key";
        String value = "test_value";
        long ttlInSec = 2;
        cache.put(key, value, ttlInSec);
        Assertions.assertEquals(1, cache.size());
    }
}
