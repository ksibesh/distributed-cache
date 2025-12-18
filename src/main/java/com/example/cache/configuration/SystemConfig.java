package com.example.cache.configuration;

import com.example.cache.core.DistributedCacheImpl;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.FirstInFirstOutStrategy;
import com.example.cache.eviction.LeastRecentUsedStrategy;
import com.example.cache.task.CacheCleanerTask;
import com.example.cache.util.SystemUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class SystemConfig {

    @Bean
    public <K, V> ConcurrentHashMap<K, CacheEntry<V>> cacheMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public <K> CacheQueue<K> cacheQueue() {
        return new CacheQueue<>(10);
    }

    @Bean
    public <K> TtlQueue<K> ttlQueue() {
        return new TtlQueue<>();
    }

    @Bean
    public <K, V> DistributedCacheImpl<K, V> distributedCache() {
        return new DistributedCacheImpl<>(cacheMap(), cacheQueue());
    }

    @Bean
    public <K> FirstInFirstOutStrategy<K> firstInFirstOutStrategy() {
        return new FirstInFirstOutStrategy<>();
    }

    @Bean
    public <K> LeastRecentUsedStrategy<K> leastFrequentlyUsedStrategy() {
        return new LeastRecentUsedStrategy<>();
    }

    @Bean
    public <K> LeastRecentUsedStrategy<K> leastRecentUsedStrategy() {
        return new LeastRecentUsedStrategy<>();
    }

    @Bean
    public <K, V> CacheCleanerTask<K, V> cacheCleanerTask() {
        int maximumSize = SystemUtil.MAX_CACHE_SIZE - SystemUtil.BREATHABLE_CACHE_SPACE;
        return new CacheCleanerTask<>(cacheQueue(), ttlQueue(), leastFrequentlyUsedStrategy(), cacheMap(),
                maximumSize);
    }
}
