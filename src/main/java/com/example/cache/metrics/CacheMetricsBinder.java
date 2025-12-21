package com.example.cache.metrics;

import com.example.cache.core.ds.CacheQueue;
import com.example.cache.core.ds.TtlQueue;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

public class CacheMetricsBinder implements MeterBinder {
    private final CacheMetrics cacheMetrics;
    private final TtlQueue ttlQueue;
    private final CacheQueue cacheQueue;

    public CacheMetricsBinder(CacheMetrics cacheMetrics, TtlQueue ttlQueue, CacheQueue cacheQueue) {
        this.cacheMetrics = cacheMetrics;
        this.ttlQueue = ttlQueue;
        this.cacheQueue = cacheQueue;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        String cacheName = "distributed.cache"; // use a consistent prefix for all cache metrics

        // --- Function Counter (Cumulative Total, pulled via getter) ---
        // Hit and Miss counter
        FunctionCounter.builder(cacheName + ".hits.total", cacheMetrics, CacheMetrics::getHits)
                .tag("operation", "get")
                .description("Total number of cache hits")
                .register(registry);
        FunctionCounter.builder(cacheName + ".misses.total", cacheMetrics, CacheMetrics::getMisses)
                .tag("operation", "get")
                .description("Total number of cache misses")
                .register(registry);

        // Operation counter
        FunctionCounter.builder(cacheName + ".puts.total", cacheMetrics, CacheMetrics::getPuts)
                .tag("operation", "put")
                .description("Total number of put operations")
                .register(registry);
        FunctionCounter.builder(cacheName + ".removes.total", cacheMetrics, CacheMetrics::getRemoves)
                .tag("operation", "remove")
                .description("Total number of remove operations")
                .register(registry);

        // Cleanup counter
        FunctionCounter.builder(cacheName + ".eviction.total", cacheMetrics, CacheMetrics::getEvictions)
                .description("Total number of keys evicted because of capacity limits")
                .register(registry);
        FunctionCounter.builder(cacheName + ".expirations.total", cacheMetrics, CacheMetrics::getTtlExpirations)
                .description("Total number of keys expired because of TTL")
                .register(registry);

        // Operation dropped counter
        FunctionCounter.builder(cacheName + ".operation.dropped.total", cacheMetrics, CacheMetrics::getDroppedOperations)
                .description("Total number of operation dropped due full queue")
                .register(registry);

        // --- Gauges (Real-time Values) ---
        // Current Cache Size
        /*Gauge.builder(cacheName + ".size", cacheMap, ConcurrentHashMap::size)
                .description("The current number of element in the cache map")
                .register(registry);*/

        // TTL Queue Size
        Gauge.builder(cacheName + ".ttl.queue.size", ttlQueue, TtlQueue::size)
                .description("The current number of element in TTL queue")
                .register(registry);

        // Cache Queue Size
        Gauge.builder(cacheName + ".cache.queue.size", cacheQueue, CacheQueue::size)
                .description("The current number of element in cache queue")
                .register(registry);

        // --- Custom Gauge (Calculated Metrics) ---
        // Hit Ratio
        Gauge.builder(cacheName + ".hit.ratio", cacheMetrics, m -> {
                    long totalRequest = m.getTotalRequests();
                    return totalRequest > 0 ? (double) m.getHits() / totalRequest : 0.0;
                })
                .description("The current hit ratio (hits / total requests")
                .register(registry);
    }
}
