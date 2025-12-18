package com.example.cache.eviction;

import java.util.LinkedHashMap;
import java.util.Optional;

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
    }

    @Override
    public void onAccess(K key) {
        lruMap.get(key);
    }

    @Override
    public void onRemove(K key) {
        lruMap.remove(key);
    }

    @Override
    public Optional<K> evict() {
        if (lruMap.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(lruMap.entrySet().iterator().next().getKey());
    }
}
