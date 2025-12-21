package com.example.cache.core.ds;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class TtlQueue {
    private final ConcurrentSkipListMap<Long, Set<String>> expirationMap;
    private final ConcurrentHashMap<String, Long> reverseIndex;

    public TtlQueue() {
        this.expirationMap = new ConcurrentSkipListMap<>();
        this.reverseIndex = new ConcurrentHashMap<>();
    }

    public void add(long ttlInSec, String key) {
        Long oldTtl = reverseIndex.get(key);
        // If key exist for another ttl, then it need to cleaned up and new ttl should be inserted.
        if (oldTtl != null) {
            removeInternal(key, oldTtl);
        }
        // check if bucket exist for the new ttl, create a new bucket if it doesn't exist, and insert the key in the ttl bucket
        expirationMap.computeIfAbsent(ttlInSec, t -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(key);
        // update the entry in the reverseIndex for future cases
        reverseIndex.put(key, ttlInSec);
        log.info("[TTLQueue.Add] [key={}] [ttlInSec={}]", key, ttlInSec);
    }

    /**
     * Helper method to remove the key from specific ttl bucket, and clear the bucket if it gets empty
     */
    private void removeInternal(String key, Long ttl) {
        Set<String> keys = expirationMap.get(ttl);
        if (keys != null) {
            keys.remove(key);
            if (keys.isEmpty()) {
                expirationMap.remove(ttl, keys);
            }
        }
    }

    public boolean isEmpty() {
        return expirationMap.isEmpty();
    }

    public Optional<Long> peek() {
        Optional<Long> peekValue =  Optional.ofNullable(expirationMap.firstKey());
        log.debug("[TTLQueue.Peek]");
        return peekValue;
    }

    public Optional<Set<String>> poll() {
        Long firstTtl = expirationMap.firstKey();
        Set<String> keys = expirationMap.get(firstTtl);

        expirationMap.remove(firstTtl, keys);
        if (keys == null || keys.isEmpty()) {
            return poll();
        }
        log.debug("[TTLQueue.Poll]");
        return Optional.of(keys);
    }

    public int size() {
        int size = expirationMap.size();
        log.debug("[TTLQueue.Size] [size={}]", size);
        return size;
    }
}
