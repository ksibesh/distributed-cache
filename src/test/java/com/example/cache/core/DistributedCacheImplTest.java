package com.example.cache.core;

import com.example.cache.cluster.IClusterService;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class DistributedCacheImplTest {

    ConcurrentHashMap<String, CacheEntry<String>> cacheMap;
    private CacheMetrics cacheMetrics;
    private CacheQueue<String> queue;
    private IClusterService<String> clusterService;

    private IDistributedCache<String, String> cache;

    private final String KEY = "test_key";
    private final String VALUE = "test_value";
    private final long TTL_IN_SEC = 60;
    private final String LOCAL_NODE_ID = "local-node-1";

    @BeforeEach
    public void setup() {
        cacheMap = mock(ConcurrentHashMap.class);
        cacheMetrics = mock(CacheMetrics.class);
        queue = (CacheQueue<String>) mock(CacheQueue.class);
        clusterService = (IClusterService<String>) mock(IClusterService.class);

        cache = new DistributedCacheImpl<>(cacheMap, queue, cacheMetrics, clusterService);
    }

    private void mockForOwnerNode() {
        when(clusterService.findOwnerNode(KEY)).thenReturn(LOCAL_NODE_ID);
        when(clusterService.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
    }

    private void mockForNonOwnerNode() {
        when(clusterService.findOwnerNode(KEY)).thenReturn("node-2");
        when(clusterService.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
    }

    @Test
    public void testPut() {
        mockForOwnerNode();

        ArgumentCaptor<CacheEntry<String>> cacheEntryCaptor = ArgumentCaptor.forClass(CacheEntry.class);
        ArgumentCaptor<CacheOperation<String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);

        cache.put(KEY, VALUE, TTL_IN_SEC);

        verify(cacheMap, times(1)).put(eq(KEY), cacheEntryCaptor.capture());
        verify(queue, times(1)).submit(cacheOperationCaptor.capture());

        CacheEntry<String> cacheEntry = cacheEntryCaptor.getValue();
        CacheOperation<String> cacheOperation = cacheOperationCaptor.getValue();

        assertEquals(VALUE, cacheEntry.getValue());
        assertEquals(CacheOperationType.PUT, cacheOperation.getType());
        assertEquals(KEY, cacheOperation.getKey());
    }

    @Test
    public void testPutWhenNotOwner() {
        mockForNonOwnerNode();

        cache.put(KEY, VALUE, TTL_IN_SEC);

        verify(queue, never()).submit(any(CacheOperation.class));
        verify(cacheMap, never()).put(eq(KEY), any(CacheEntry.class));
    }

    @Test
    public void testPutWithNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            cache.put(null, VALUE, TTL_IN_SEC);
        });
    }

    @Test
    public void testPutWithNullValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            cache.put(KEY, null, TTL_IN_SEC);
        });
    }

    @Test
    public void testGet() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long expirationTime = currentTimeInSec + TTL_IN_SEC;

        mockForOwnerNode();

        CacheEntry<String> entry = CacheEntry.<String>builder()
                .value(VALUE)
                .expirationTime(expirationTime)
                .build();
        when(cacheMap.get(KEY)).thenReturn(entry);

        ArgumentCaptor<CacheOperation<String>> operationCaptor = ArgumentCaptor.forClass(CacheOperation.class);

        String returnedValue = cache.get(KEY);

        assertEquals(VALUE, returnedValue);
        verify(cacheMap, times(1)).get(KEY);
        verify(queue, times(1)).submit(operationCaptor.capture());

        CacheOperation<String> submittedOperation = operationCaptor.getValue();
        assertEquals(CacheOperationType.ACCESS, submittedOperation.getType());
        assertEquals(KEY, submittedOperation.getKey());
    }

    @Test
    public void testGetAfterTtl() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long expirationTimeInSec = currentTimeInSec - TTL_IN_SEC;

        try (MockedStatic<SystemUtil> mockedSystemUtil = mockStatic(SystemUtil.class)) {
            mockedSystemUtil.when(SystemUtil::getCurrentTimeInSec).thenReturn(currentTimeInSec);

            mockForOwnerNode();

            CacheEntry<String> cacheEntry = CacheEntry.<String>builder()
                    .value(VALUE)
                    .expirationTime(expirationTimeInSec)
                    .build();
            when(cacheMap.get(KEY)).thenReturn(cacheEntry);

            String returnedValue = cache.get(KEY);

            ArgumentCaptor<CacheOperation<String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);

            verify(cacheMap, times(1)).remove(KEY);
            verify(queue, times(1)).submit(cacheOperationCaptor.capture());

            CacheOperation<String> cacheOperation = cacheOperationCaptor.getValue();

            assertNull(returnedValue);
            assertEquals(CacheOperationType.REMOVE, cacheOperation.getType());
            assertEquals(KEY, cacheOperation.getKey());
        }
    }

    @Test
    public void testGetWithNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            cache.get(null);
        });
    }

    @Test
    public void testGetForCacheMiss() {
        mockForOwnerNode();
        when(cacheMap.get(KEY)).thenReturn(null);

        cache.get(KEY);

        verify(cacheMap, times(1)).get(KEY);
        verify(queue, never()).submit(any(CacheOperation.class));
        verify(cacheMetrics, times(1)).incrementMisses();
    }

    @Test
    public void testGetWhenNotOwner() {
        mockForNonOwnerNode();

        cache.get(KEY);

        verify(cacheMap, never()).get(KEY);
        verify(queue, never()).submit(any(CacheOperation.class));
    }

    @Test
    public void testDelete() {
        mockForOwnerNode();
        when(cacheMap.containsKey(KEY)).thenReturn(true);

        cache.delete(KEY);

        ArgumentCaptor<CacheOperation<String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);

        verify(cacheMetrics, times(1)).incrementRemoves();
        verify(cacheMap, times(1)).remove(KEY);
        verify(queue, times(1)).submit(cacheOperationCaptor.capture());

        CacheOperation<String> cacheOperation = cacheOperationCaptor.getValue();
        assertEquals(CacheOperationType.REMOVE, cacheOperation.getType());
        assertEquals(KEY, cacheOperation.getKey());
    }

    @Test
    public void testDeleteWithNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.delete(null));
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
        mockForOwnerNode();
        when(cacheMap.containsKey(KEY)).thenReturn(false);

        cache.delete(KEY);

        ArgumentCaptor<CacheOperation<String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);

        verify(cacheMetrics, never()).incrementRemoves();
        verify(cacheMap, times(1)).remove(KEY);
        verify(queue, times(1)).submit(cacheOperationCaptor.capture());

        CacheOperation<String> cacheOperation = cacheOperationCaptor.getValue();
        assertEquals(CacheOperationType.REMOVE, cacheOperation.getType());
        assertEquals(KEY, cacheOperation.getKey());
    }

    @Test
    public void testDeleteWhenNotOwner() {
        mockForNonOwnerNode();

        cache.delete(KEY);

        verify(cacheMetrics, never()).incrementRemoves();
        verify(cacheMap, never()).remove(KEY);
        verify(queue, never()).submit(any(CacheOperation.class));
    }
}
