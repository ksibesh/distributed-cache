package com.example.cache.core;

import com.example.cache.cluster.IClusterService;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class SingleThreadedCacheCoreTest {

    private CacheQueue<String, String> queue;
    private CacheMetrics cacheMetrics;
    private IClusterService<String> clusterService;

    private SingleThreadedCacheCore<String, String> cacheCore;

    private final String TEST_KEY = "testKey";
    private final String TEST_VALUE = "testValue";
    private final String LOCAL_NODE_ID = "local-node-1";
    private final long TTL = 60;

    @BeforeEach
    public void setup() {
        queue = (CacheQueue<String, String>) mock(CacheQueue.class);
        cacheMetrics = mock(CacheMetrics.class);
        clusterService = (IClusterService<String>) mock(IClusterService.class);

        cacheCore = new SingleThreadedCacheCore<>("main-worker-thread", queue, cacheMetrics, clusterService);
    }

    private void mockForOwnerNode(String... keys) {
        for (String key : keys) {
            when(clusterService.findOwnerNode(key)).thenReturn(LOCAL_NODE_ID);
        }
        when(clusterService.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
    }

    private void mockForNonOwnerNode(String key) {
        when(clusterService.findOwnerNode(key)).thenReturn("node-2");
        when(clusterService.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testPutAndGetSuccess() throws Exception {
        mockForOwnerNode(TEST_KEY);

        CompletableFuture<Void> putFuture = cacheCore.submitPut(TEST_KEY, TEST_VALUE, TTL);
        putFuture.get();

        {
            ArgumentCaptor<CacheOperation<String, String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);
            assertEquals(1, cacheCore.size());
            verify(cacheMetrics, times(1)).incrementPuts();
            verify(queue, times(1)).submit(cacheOperationCaptor.capture());

            CacheOperation<String, String> cacheOperation = cacheOperationCaptor.getValue();
            assertEquals(TEST_KEY, cacheOperation.getKey());
            assertEquals(CacheOperationType.PUT, cacheOperation.getType());
            assertEquals(TEST_VALUE, cacheOperation.getEntry().getValue());
        }

        CompletableFuture<String> getFuture = cacheCore.submitGet(TEST_KEY);
        String result = getFuture.get();

        {
            ArgumentCaptor<CacheOperation<String, String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);
            assertEquals(TEST_VALUE, result);
            verify(cacheMetrics, times(1)).incrementHits();
            // 2 times, coz it was called once in the previous block as well
            verify(queue, times(2)).submit(cacheOperationCaptor.capture());

            CacheOperation<String, String> cacheOperation = cacheOperationCaptor.getValue();
            assertEquals(CacheOperationType.GET, cacheOperation.getType());
            assertEquals(TEST_KEY, cacheOperation.getKey());
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testGetCacheMiss() throws Exception {
        String nonExistentKey = "non-existent-key";
        mockForOwnerNode(nonExistentKey);

        CompletableFuture<String> getResult = cacheCore.submitGet(nonExistentKey);
        String result = getResult.get();

        assertNull(result);
        verify(cacheMetrics, times(1)).incrementMisses();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testDeleteForExistingKey() throws Exception {
        mockForOwnerNode(TEST_KEY);

        CompletableFuture<Void> putResult = cacheCore.submitPut(TEST_KEY, TEST_VALUE, TTL);
        putResult.get();
        assertEquals(1, cacheCore.size());

        CompletableFuture<Void> deleteResult = cacheCore.submitDelete(TEST_KEY);
        deleteResult.get();
        assertEquals(0, cacheCore.size());
        verify(cacheMetrics, times(1)).incrementRemoves();
        ArgumentCaptor<CacheOperation<String, String>> cacheOperationCaptor = ArgumentCaptor.forClass(CacheOperation.class);
        // 2 times, coz it must have been called at get as well
        verify(queue, times(2)).submit(cacheOperationCaptor.capture());
        CacheOperation<String, String> cacheOperation = cacheOperationCaptor.getValue();
        assertEquals(CacheOperationType.DELETE, cacheOperation.getType());
        assertEquals(TEST_KEY, cacheOperation.getKey());
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testDeleteForNonExistingKey() throws Exception {
        String nonExistentKey = "non-existent-key";
        mockForOwnerNode(TEST_KEY, nonExistentKey);

        CompletableFuture<Void> putResult = cacheCore.submitPut(TEST_KEY, TEST_VALUE, TTL);
        putResult.get();
        assertEquals(1, cacheCore.size());

        CompletableFuture<Void> deleteResult = cacheCore.submitDelete(nonExistentKey);
        deleteResult.get();
        assertEquals(1, cacheCore.size());
    }

    // @Test
    // TODO: Fix this test case later
    public void testGetExpired() throws Exception {
        mockForOwnerNode(TEST_KEY);

        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long expiredTimeInSec = currentTimeInSec - (2 * TTL);
        long futureTimeInSec = currentTimeInSec + TTL;

        try (MockedStatic<SystemUtil> mockedSystemUtil = mockStatic(SystemUtil.class)) {
            mockedSystemUtil.when(SystemUtil::getCurrentTimeInSec).thenReturn(expiredTimeInSec);
            CompletableFuture<Void> putResult = cacheCore.submitPut(TEST_KEY, TEST_VALUE, TTL);
            putResult.get();
            assertEquals(1, cacheCore.size());  // verified this so that in next step we can verify this becoming 0 during get

            mockedSystemUtil.when(SystemUtil::getCurrentTimeInSec).thenReturn(futureTimeInSec);
            CompletableFuture<String> getResult = cacheCore.submitGet(TEST_KEY);
            String result = getResult.get();

            assertNull(result);
            assertEquals(0, cacheCore.size());
            verify(cacheMetrics, times(1)).incrementTtlExpirations();
            verify(cacheMetrics, times(1)).incrementRemoves();

        }
    }

    @Test
    public void testRoutingWhenNotOwner() throws Exception {
        mockForNonOwnerNode(TEST_KEY);
        cacheCore.submitPut(TEST_KEY, TEST_VALUE, TTL).get();

        assertEquals(0, cacheCore.size());
        verify(cacheMetrics, never()).incrementPuts();

        String result = cacheCore.submitGet(TEST_KEY).get();
        assertNull(result);
    }

    @Test
    public void testConcurrentSubmission() throws Exception {
        mockForOwnerNode("k1", "k2", "k3");
        CompletableFuture<Void> f1 = cacheCore.submitPut("k1", "v1", 10);
        CompletableFuture<Void> f2 = cacheCore.submitPut("k2", "v2", 10);
        CompletableFuture<Void> f3 = cacheCore.submitPut("k3", "v3", 10);

        CompletableFuture.allOf(f1, f2, f3).get();

        assertEquals(3, cacheCore.size());
        verify(cacheMetrics, times(3)).incrementPuts();

    }

}
