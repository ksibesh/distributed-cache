package com.example.cache.core;

import java.util.concurrent.CompletableFuture;

public interface IDistributedCache<K, V> {

    CompletableFuture<Void> submitPut(K key, V value, long ttlSeconds);

    CompletableFuture<V> submitGet(K key);

    CompletableFuture<Void> submitDelete(K key);

    int size();

}
