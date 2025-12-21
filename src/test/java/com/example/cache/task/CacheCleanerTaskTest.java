package com.example.cache.task;

import com.example.cache.core.IDistributedCache;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.IEvictionStrategy;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.util.SystemUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class CacheCleanerTaskTest {

    private CacheQueue cacheQueue;
    private TtlQueue ttlQueue;
    private IEvictionStrategy<String> evictionStrategy;
    private IDistributedCache cacheCore;
    private CacheMetrics cacheMetrics;

    private CacheCleanerTask cacheCleanerTask;

    private final String testKey = "testKey";
    private final int maxCacheSize = 10;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        cacheQueue = mock(CacheQueue.class);
        ttlQueue = mock(TtlQueue.class);
        evictionStrategy = (IEvictionStrategy<String>) mock(IEvictionStrategy.class);
        cacheMetrics = mock(CacheMetrics.class);
        cacheCore = mock(IDistributedCache.class);

        cacheCleanerTask = new CacheCleanerTask(cacheQueue, ttlQueue, evictionStrategy, maxCacheSize, cacheMetrics, cacheCore);
    }

    private void runTaskCycle(Optional<CacheOperation> operation) {
        when(cacheQueue.poll(anyLong(), any(TimeUnit.class)))
                .thenReturn(operation)
                .thenAnswer(invocation -> {
                    cacheCleanerTask.stop();
                    return Optional.empty();
                });
        cacheCleanerTask.run();
    }

    @Test
    public void testDispatchOperationForPut() {
        long expirationTime = SystemUtil.getCurrentTimeInSec() + 60;
        CacheEntry entry = CacheEntry.builder()
                .expirationTime(expirationTime)
                .build();
        CacheOperation putOperation = CacheOperation.of(CacheOperationType.PUT, testKey, entry);

        runTaskCycle(Optional.of(putOperation));

        verify(evictionStrategy, times(1)).onPut(testKey);
        verify(ttlQueue, times(1)).add(expirationTime, testKey);

        verify(evictionStrategy, never()).onGet(anyString());
        verify(evictionStrategy, never()).onDelete(anyString());
    }

    @Test
    public void testDispatchOperationForAccess() {
        CacheOperation accessOperation = CacheOperation.of(CacheOperationType.GET, testKey);

        runTaskCycle(Optional.of(accessOperation));

        verify(evictionStrategy, times(1)).onGet(testKey);

        verify(evictionStrategy, never()).onPut(anyString());
        verify(evictionStrategy, never()).onDelete(anyString());
    }

    @Test
    public void testDispatchOperationForRemove() {
        CacheOperation removeOperation = CacheOperation.of(CacheOperationType.DELETE, testKey);

        runTaskCycle(Optional.of(removeOperation));

        verify(evictionStrategy, times(1)).onDelete(testKey);

        verify(evictionStrategy, never()).onPut(anyString());
        verify(evictionStrategy, never()).onGet(anyString());
    }

    @Test
    public void testCleanUpExpiredKeys() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long expiredTimeInSec = currentTimeInSec - 10;
        long nonExpiredTimeInSec = currentTimeInSec + 10;
        String expiredKey1 = "expiredKey1";
        String expiredKey2 = "expiredKey2";
        String nonExpiredKey1 = "nonExpiredKey1";
        Set<String> expiredKeySet = Set.of(expiredKey1, expiredKey2);

        try (MockedStatic<SystemUtil> mockedSystemUtils = mockStatic(SystemUtil.class)) {
            mockedSystemUtils.when(SystemUtil::getCurrentTimeInSec).thenReturn(currentTimeInSec);
            when(ttlQueue.isEmpty()).thenReturn(false, false, true);
            when(ttlQueue.peek()).thenReturn(Optional.of(expiredTimeInSec))
                    .thenReturn(Optional.of(nonExpiredTimeInSec));
            when(ttlQueue.poll()).thenReturn(Optional.of(expiredKeySet));


            runTaskCycle(Optional.empty());

            verify(evictionStrategy, times(1)).onDelete(expiredKey1);
            verify(evictionStrategy, times(1)).onDelete(expiredKey2);
            verify(cacheCore, times(1)).submitDelete(expiredKey1);
            verify(cacheCore, times(1)).submitDelete(expiredKey2);
            verify(evictionStrategy, never()).onDelete(nonExpiredKey1);
            verify(cacheMetrics, times(2)).incrementTtlExpirations();
        }
    }

    @Test
    public void testCleanupExpiredKeysWithNoExpiringKeys() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long nonExpiredTimeInSec = currentTimeInSec + 10;
        String nonExpiredKey1 = "nonExpiredKey1";
        Set<String> nonExpiredKeySet = Set.of(nonExpiredKey1);

        try (MockedStatic<SystemUtil> mockedSystemUtils = mockStatic(SystemUtil.class)) {
            mockedSystemUtils.when(SystemUtil::getCurrentTimeInSec).thenReturn(currentTimeInSec);
            when(ttlQueue.isEmpty()).thenReturn(false);
            when(ttlQueue.peek()).thenReturn(Optional.of(nonExpiredTimeInSec));
            when(ttlQueue.poll()).thenReturn(Optional.of(nonExpiredKeySet));

            runTaskCycle(Optional.empty());

            verify(ttlQueue, never()).poll();
            verify(evictionStrategy, never()).onDelete(nonExpiredKey1);
            verify(cacheCore, never()).submitDelete(nonExpiredKey1);
            verify(cacheMetrics, never()).incrementTtlExpirations();
        }
    }

    @Test
    public void testEnforceCapacityLimit() {
        when(cacheCore.size()).thenReturn(maxCacheSize + 1).thenReturn(maxCacheSize);
        when(evictionStrategy.evict()).thenReturn(Optional.of(testKey));

        runTaskCycle(Optional.empty());

        verify(evictionStrategy, times(1)).evict();
        verify(cacheCore, times(1)).submitDelete(testKey);
        verify(evictionStrategy, times(1)).onDelete(testKey);
        verify(cacheMetrics, times(1)).incrementEvictions();
    }

    @Test
    public void testEnforceCapacityLimitWhenSizeLessThanLimit() {
        when(cacheCore.size()).thenReturn(maxCacheSize - 1);
        when(evictionStrategy.evict()).thenReturn(Optional.of(testKey));

        runTaskCycle(Optional.empty());

        verify(evictionStrategy, never()).evict();
        verify(evictionStrategy, never()).onDelete(testKey);
        verify(cacheCore, never()).submitDelete(testKey);
        verify(cacheMetrics, never()).incrementEvictions();
    }

    @Test
    public void testEnforceCapacityLimitWhenSizeAboveLimitButIssueWithEviction() {
        // intentionally haven't placed another thenReturn in cascade, coz ideally the loop should be broken by break clause
        when(cacheCore.size()).thenReturn(maxCacheSize + 1);
        when(evictionStrategy.evict()).thenReturn(Optional.empty());

        runTaskCycle(Optional.empty());

        // here times have value 2 coz runTaskCycle runs it 2 times
        verify(evictionStrategy, times(2)).evict();
        verify(evictionStrategy, never()).onDelete(anyString());
        verify(cacheCore, never()).submitDelete(anyString());
        verify(cacheMetrics, never()).incrementEvictions();
    }

}
