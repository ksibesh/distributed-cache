package com.example.cache.eviction;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Optional;

@Slf4j
public class LeastRecentUsedStrategy<K> implements IEvictionStrategy<K> {

    // Using this object for dummy value, as we do need to store value at all, that's the reason it is defined
    // at class level so that same heap object can be used in the map, saving memory for the system.
    private final Object DEFAULT_VALUE = new Object();

    private final LinkedHashMap<K, Object> lruMap;

    public LeastRecentUsedStrategy() {
        this.lruMap = new LinkedHashMap<>(16, 0.75f, true);
    }

    @Override
    public void onPut(K key) {
        lruMap.put(key, DEFAULT_VALUE);
        log.debug("[Eviction.Strategy.LRU.PUT] [key={}]", key);
    }

    @Override
    public void onAccess(K key) {
        lruMap.get(key);
        log.debug("[Eviction.Strategy.LRU.ACCESS] [key={}]", key);
    }

    @Override
    public void onRemove(K key) {
        lruMap.remove(key);
        log.debug("[Eviction.Strategy.LRU.REMOVE] [key={}]", key);
    }

    @Override
    public Optional<K> evict() {
        if (lruMap.isEmpty()) {
            log.debug("[Eviction.Strategy.LRU.EVICT] [<empty>]");
            return Optional.empty();
        }
        Optional<K> evictionEntry = Optional.of(lruMap.entrySet().iterator().next().getKey());
        log.debug("[Eviction.Strategy.LRU.EVICT]");
        return evictionEntry;
    }
}
