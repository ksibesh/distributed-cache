package com.example.cache.metrics;

import lombok.Getter;

@Getter
public class CacheMetrics {
    private long hits;
    private long misses;
    private long puts;
    private long removes;
    private long evictions;
    private long ttlExpirations;
    private long droppedOperations;

    public void incrementHits() {
        hits++;
    }

    public void incrementMisses() {
        misses++;
    }

    public void incrementPuts() {
        puts++;
    }

    public void incrementRemoves() {
        removes++;
    }

    public void incrementEvictions() {
        evictions++;
    }

    public void incrementTtlExpirations() {
        ttlExpirations++;
    }

    public void incrementDroppedOperations() {
        droppedOperations++;
    }

    public long getTotalRequests() {
        return hits + misses;
    }
}

