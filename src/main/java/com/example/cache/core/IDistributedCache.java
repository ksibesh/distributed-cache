package com.example.cache.core;

public interface IDistributedCache<K, V> {

    void put(K key, V value, long ttlSeconds);

    V get(K key);

    void delete(K key);

}
