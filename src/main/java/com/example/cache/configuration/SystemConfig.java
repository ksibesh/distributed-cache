package com.example.cache.configuration;

import com.example.cache.core.DistributedCacheImpl;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.FirstInFirstOutStrategy;
import com.example.cache.eviction.LeastRecentUsedStrategy;
import com.example.cache.task.CacheCleanerTask;
import com.example.cache.task.CacheCleanerTaskInitializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class SystemConfig {

    @Bean
    public ConcurrentHashMap<String, CacheEntry<String>> cacheMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public CacheQueue<String> cacheQueue() {
        return new CacheQueue<>(10);
    }

    @Bean
    public TtlQueue<String> ttlQueue() {
        return new TtlQueue<>();
    }

    @Bean
    public DistributedCacheImpl<String ,String> distributedCache() {
        return new DistributedCacheImpl<>(cacheMap(), cacheQueue());
    }

    @Bean
    public FirstInFirstOutStrategy<String> firstInFirstOutStrategy() {
        return new FirstInFirstOutStrategy<>();
    }

    @Bean
    public LeastRecentUsedStrategy<String> leastFrequentlyUsedStrategy() {
        return new LeastRecentUsedStrategy<>();
    }

    @Bean
    public LeastRecentUsedStrategy<String> leastRecentUsedStrategy() {
        return new LeastRecentUsedStrategy<>();
    }

    @Bean
    public CacheCleanerTask<String, String> cacheCleanerTask(
            @Value("${cache.max-size:1000}") int maxCacheSize,
            @Value("${cache.breathable-space:100}") int breathableSpace
    ) {
        int maximumSize = maxCacheSize - breathableSpace;
        return new CacheCleanerTask<>(
                cacheQueue(),
                ttlQueue(),
                leastFrequentlyUsedStrategy(),
                cacheMap(),
                maximumSize
        );
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public CacheCleanerTaskInitializer cacheCleanerTaskInitializer(
            CacheCleanerTask<String, String> cacheCleanerTask,
            @Value("${cache.cleaner.threads:1}") int cleanerThreadPoolSize
    ) {
        return new CacheCleanerTaskInitializer(cacheCleanerTask, cleanerThreadPoolSize);
    }
}
