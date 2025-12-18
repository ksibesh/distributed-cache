package com.example.cache.eviction.domain;

import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

@Builder
@Getter
public class LeastFrequentlyUsedMetadata {
    @Builder.Default
    private AtomicInteger frequency = new AtomicInteger(1);

    public int getFrequencyValue() {
        return this.frequency.get();
    }
}
