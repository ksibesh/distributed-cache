package com.example.cache.eviction;

import java.util.Optional;

public interface IEvictionStrategy<K> {

    void onPut(K key);

    void onGet(K key);

    void onDelete(K key);

    Optional<K> evict();
}
