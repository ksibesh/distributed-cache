package com.example.cache.core;

import java.util.concurrent.CompletableFuture;

public interface IDistributedCache {

    CompletableFuture<Void> submitPut(String key, String value, long ttlSeconds);

    CompletableFuture<String> submitGet(String key);

    CompletableFuture<Void> submitDelete(String key);

    int size();

}
