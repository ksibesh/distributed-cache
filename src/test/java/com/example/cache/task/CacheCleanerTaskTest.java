package com.example.cache.task;

import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.domain.CacheOperation;
import com.example.cache.core.domain.CacheOperationType;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.IEvictionStrategy;
import com.example.cache.util.SystemUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class CacheCleanerTaskTest {

    private CacheQueue<String> cacheQueue;
    private TtlQueue<String> ttlQueue;
    private IEvictionStrategy<String> evictionStrategy;
    private ConcurrentHashMap<String, CacheEntry<String>> cacheMap;

    private CacheCleanerTask<String, String> cacheCleanerTask;

    private final String testKey = "testKey";
    private final String testValue = "testValue";
    private final int maxCacheSize = 10;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        cacheQueue = (CacheQueue<String>) mock(CacheQueue.class);
        ttlQueue = (TtlQueue<String>) mock(TtlQueue.class);
        evictionStrategy = (IEvictionStrategy<String>) mock(IEvictionStrategy.class);
        cacheMap = (ConcurrentHashMap<String, CacheEntry<String>>) mock(ConcurrentHashMap.class);

        cacheCleanerTask = new CacheCleanerTask<>(cacheQueue, ttlQueue, evictionStrategy, cacheMap, maxCacheSize);
    }

    private void runTaskCycle(Optional<CacheOperation<String>> operation) {
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
        CacheOperation<String> putOperation = CacheOperation.of(CacheOperationType.PUT, testKey);
        CacheEntry<String> entry = CacheEntry.<String>builder().expirationTime(expirationTime).value(testValue).build();

        when(cacheMap.get(testKey)).thenReturn(entry);

        runTaskCycle(Optional.of(putOperation));

        verify(evictionStrategy, times(1)).onPut(testKey);
        verify(cacheMap, times(1)).get(testKey);
        verify(ttlQueue, times(1)).add(expirationTime, testKey);

        verify(evictionStrategy, never()).onAccess(anyString());
        verify(evictionStrategy, never()).onRemove(anyString());
    }

    @Test
    public void testDispatchOperationForAccess() {
        CacheOperation<String> accessOperation = CacheOperation.of(CacheOperationType.ACCESS, testKey);

        runTaskCycle(Optional.of(accessOperation));

        verify(evictionStrategy, times(1)).onAccess(testKey);

        verify(evictionStrategy, never()).onPut(anyString());
        verify(evictionStrategy, never()).onRemove(anyString());
    }

    @Test
    public void testDispatchOperationForRemove() {
        CacheOperation<String> removeOperation = CacheOperation.of(CacheOperationType.REMOVE, testKey);

        runTaskCycle(Optional.of(removeOperation));

        verify(evictionStrategy, times(1)).onRemove(testKey);

        verify(evictionStrategy, never()).onPut(anyString());
        verify(evictionStrategy, never()).onAccess(anyString());
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

            when(cacheMap.containsKey(expiredKey1)).thenReturn(true);
            when(cacheMap.containsKey(expiredKey2)).thenReturn(true);
            when(cacheMap.containsKey(nonExpiredKey1)).thenReturn(true);

            runTaskCycle(Optional.empty());

            verify(cacheMap, never()).containsKey(nonExpiredKey1);
            verify(cacheMap, times(1)).remove(expiredKey1);
            verify(cacheMap, times(1)).remove(expiredKey2);
            verify(cacheMap, never()).remove(nonExpiredKey1);
            verify(evictionStrategy, times(1)).onRemove(expiredKey1);
            verify(evictionStrategy, times(1)).onRemove(expiredKey2);
            verify(evictionStrategy, never()).onRemove(nonExpiredKey1);
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

            when(cacheMap.containsKey(nonExpiredKey1)).thenReturn(true);

            runTaskCycle(Optional.empty());

            verify(cacheMap, never()).containsKey(nonExpiredKey1);
            verify(cacheMap, never()).remove(nonExpiredKey1);
            verify(evictionStrategy, never()).onRemove(nonExpiredKey1);
        }
    }

    @Test
    public void testEnforceCapacityLimit() {
        when(cacheMap.size()).thenReturn(maxCacheSize + 1).thenReturn(maxCacheSize);
        when(evictionStrategy.evict()).thenReturn(Optional.of(testKey));

        runTaskCycle(Optional.empty());

        verify(cacheMap, times(1)).remove(testKey);
        verify(evictionStrategy, times(1)).onRemove(testKey);
    }

    @Test
    public void testEnforceCapacityLimitWhenSizeLessThanLimit() {
        when(cacheMap.size()).thenReturn(maxCacheSize);
        when(evictionStrategy.evict()).thenReturn(Optional.of(testKey));

        runTaskCycle(Optional.empty());

        verify(cacheMap, never()).remove(testKey);
        verify(evictionStrategy, never()).onRemove(testKey);
    }

    @Test
    public void testEnforceCapacityLimitWhenSizeAboveLimitButIssueWithEviction() {
        // intentionally haven't placed another thenReturn in cascade, coz ideally the loop should be broken by break clause
        when(cacheMap.size()).thenReturn(maxCacheSize + 1);
        when(evictionStrategy.evict()).thenReturn(Optional.empty());

        runTaskCycle(Optional.empty());

        verify(cacheMap, never()).remove(testKey);
        verify(evictionStrategy, never()).onRemove(testKey);
    }

}
