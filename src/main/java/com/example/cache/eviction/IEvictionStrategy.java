package com.example.cache.eviction;

import java.util.Optional;

public interface IEvictionStrategy<K> {

    void onPut(K key);

    void onAccess(K key);

    void onRemove(K key);

    Optional<K> evict();
}
