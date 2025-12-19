package com.example.cache.configuration;

import com.example.cache.cluster.ConsistentHashClusterService;
import com.example.cache.cluster.IClusterService;
import com.example.cache.core.IDistributedCache;
import com.example.cache.core.SingleThreadedCacheCore;
import com.example.cache.core.domain.CacheEntry;
import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import com.example.cache.eviction.FirstInFirstOutStrategy;
import com.example.cache.eviction.LeastRecentUsedStrategy;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.metrics.CacheMetricsBinder;
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
    public TtlQueue<String> ttlQueue() {
        return new TtlQueue<>();
    }

    @Bean
    public CacheMetrics cacheMetrics() {
        return new CacheMetrics();
    }

    @Bean
    public CacheQueue<String, String> cacheQueue() {
        return new CacheQueue<>(10, cacheMetrics());
    }

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CacheMetricsBinder cacheMetricsBinder(CacheMetrics cacheMetrics,
                                                 ConcurrentHashMap cacheMap,
                                                 TtlQueue<String> ttlQueue, CacheQueue<String, String> cacheQueue) {
        return new CacheMetricsBinder(cacheMetrics, cacheMap, ttlQueue, cacheQueue);
    }

    @Bean
    public IClusterService<String> clusterService(
            @Value("${cluster.node.id:node-1}") String localNodeId,
            @Value("${cluster.virtual.nodes:10}") int virtualNodesPerNode,
            @Value("${cluster.initial.nodes:node-1,node-2,node-3}") String initialNodeCsv
    ) {
        ConsistentHashClusterService<String> clusterService = new ConsistentHashClusterService<>(localNodeId, virtualNodesPerNode);
        String[] nodes = initialNodeCsv.split(",");
        for (String node : nodes) {
            clusterService.addNode(node.trim());
        }
        return clusterService;
    }

    @Bean
    public IDistributedCache<String, String> singleThreadedCacheCore(
            @Value("${cache.name:core-worker-thread}") String workerThreadName,
            IClusterService<String> clusterService
    ) {
        return new SingleThreadedCacheCore<>(workerThreadName, cacheQueue(), cacheMetrics(), clusterService);
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
            @Value("${cache.breathable-space:100}") int breathableSpace,
            IDistributedCache<String, String> singleThreadedCacheCore
    ) {
        int maximumSize = maxCacheSize - breathableSpace;
        return new CacheCleanerTask<>(
                cacheQueue(),
                ttlQueue(),
                leastFrequentlyUsedStrategy(),
                maximumSize,
                cacheMetrics(),
                singleThreadedCacheCore
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
