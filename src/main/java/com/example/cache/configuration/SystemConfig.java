package com.example.cache.configuration;

import com.example.cache.cluster.ConsistentHashClusterService;
import com.example.cache.cluster.IClusterService;
import com.example.cache.cluster.grpc.CacheGrpcClient;
import com.example.cache.core.IDistributedCache;
import com.example.cache.core.SingleThreadedCacheCore;
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
    public TtlQueue ttlQueue() {
        return new TtlQueue();
    }

    @Bean
    public CacheQueue cacheQueue() {
        return new CacheQueue(10, cacheMetrics());
    }

    @Bean
    public CacheMetrics cacheMetrics() {
        return new CacheMetrics();
    }

    @Bean
    public CacheMetricsBinder cacheMetricsBinder(CacheMetrics cacheMetrics, TtlQueue ttlQueue, CacheQueue cacheQueue) {
        return new CacheMetricsBinder(cacheMetrics, ttlQueue, cacheQueue);
    }

    @Bean
    public IClusterService clusterService(
            @Value("${cluster.node.id:node-1}") String localNodeId,
            @Value("${cluster.virtual.nodes:10}") int virtualNodesPerNode,
            @Value("${cluster.initial.nodes:node-1:0.0.0.0,node-2:0.0.0.0,node-3:0.0.0.0}") String initialNodeCsv
    ) {
        ConsistentHashClusterService clusterService = new ConsistentHashClusterService(localNodeId, virtualNodesPerNode);
        String[] nodes = initialNodeCsv.split(",");
        for (String node : nodes) {
            String[] nodeIdAddressPair = node.split(":");
            clusterService.addNode(nodeIdAddressPair[0].trim(), nodeIdAddressPair[1].trim());
        }
        return clusterService;
    }

    @Bean
    public CacheGrpcClient cacheGrpcClient() {
        return new CacheGrpcClient();
    }

    @Bean
    public IDistributedCache singleThreadedCacheCore(
            @Value("${cache.name:core-worker-thread}") String workerThreadName,
            IClusterService clusterService,
            CacheGrpcClient cacheGrpcClient
    ) {
        return new SingleThreadedCacheCore(workerThreadName, cacheQueue(), cacheMetrics(), clusterService, cacheGrpcClient);
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
    public CacheCleanerTask cacheCleanerTask(
            @Value("${cache.max-size:1000}") int maxCacheSize,
            @Value("${cache.breathable-space:100}") int breathableSpace,
            IDistributedCache singleThreadedCacheCore
    ) {
        int maximumSize = maxCacheSize - breathableSpace;
        return new CacheCleanerTask(
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
            CacheCleanerTask cacheCleanerTask,
            @Value("${cache.cleaner.threads:1}") int cleanerThreadPoolSize
    ) {
        return new CacheCleanerTaskInitializer(cacheCleanerTask, cleanerThreadPoolSize);
    }
}
