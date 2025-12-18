package com.example.cache.eviction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class LeastRecentUsedStrategyTest {

    private IEvictionStrategy<String> lruEvictionStrategy;

    @BeforeEach
    public void setUp() {
        lruEvictionStrategy = new LeastRecentUsedStrategy<>();
    }

    @Test
    public void testLruItemWithMultipleElements() {
        String[] keys = new String[] {"test_key_1", "test_key_2", "test_key_3"};
        lruEvictionStrategy.onPut(keys[0]);
        lruEvictionStrategy.onPut(keys[1]);
        lruEvictionStrategy.onPut(keys[2]);

        lruEvictionStrategy.onAccess(keys[0]);
        lruEvictionStrategy.onAccess(keys[2]);
        lruEvictionStrategy.onAccess(keys[0]);

        Optional<String> firstEvictedItem = lruEvictionStrategy.evict();
        Assertions.assertTrue(firstEvictedItem.isPresent());
        Assertions.assertEquals(keys[1], firstEvictedItem.get());
    }

    @Test
    public void testMultipleLruItemWithBigKeySetUntilEmpty() {
        String[] keys = new String[] {"test_key_1", "test_key_2", "test_key_3", "test_key_4", "test_key_5"};
        lruEvictionStrategy.onPut(keys[0]);
        lruEvictionStrategy.onPut(keys[1]);
        lruEvictionStrategy.onPut(keys[2]);
        lruEvictionStrategy.onPut(keys[3]);
        lruEvictionStrategy.onPut(keys[4]);
        // flow: (LRU) 0 -> 1 -> 2 -> 3 -> 4 (MRU)

        lruEvictionStrategy.onAccess(keys[0]);  // flow: (LRU) 1 -> 2 -> 3 -> 4 -> 0 (MRU)
        lruEvictionStrategy.onAccess(keys[4]);  // flow: (LRU) 1 -> 2 -> 3 -> 0 -> 4 (MRU)
        lruEvictionStrategy.onAccess(keys[2]);  // flow: (LRU) 1 -> 3 -> 0 -> 4 -> 2 (MRU)

        {
            Optional<String> evictedItem = lruEvictionStrategy.evict();    // flow: (LRU) [1] 3 -> 0 -> 4 -> 2 (MRU)
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[1], evictedItem.get());
            lruEvictionStrategy.onRemove(evictedItem.get());
        }

        lruEvictionStrategy.onAccess(keys[3]);  // flow: (LRU) 0 -> 4 -> 2 -> 3 (MRU)
        lruEvictionStrategy.onAccess(keys[0]);  // flow: (LRU) 4 -> 2 -> 3 -> 0 (MRU)

        {
            Optional<String> evictedItem = lruEvictionStrategy.evict();    // flow: (LRU) [4] 2 -> 3 -> 0 (MRU)
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[4], evictedItem.get());
            lruEvictionStrategy.onRemove(evictedItem.get());
        }

        {
            Optional<String> evictedItem = lruEvictionStrategy.evict();    // flow: (LRU) [2] 3 -> 0 (MRU)
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[2], evictedItem.get());
            lruEvictionStrategy.onRemove(evictedItem.get());
        }

        {
            Optional<String> evictedItem = lruEvictionStrategy.evict();    // flow: (LRU) [3] 0 (MRU)
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[3], evictedItem.get());
            lruEvictionStrategy.onRemove(evictedItem.get());
        }

        {
            Optional<String> evictedItem = lruEvictionStrategy.evict();    // flow: (LRU) [0] (MRU)
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[0], evictedItem.get());
            lruEvictionStrategy.onRemove(evictedItem.get());
        }

        {
            Optional<String> evictedItem = lruEvictionStrategy.evict();    // flow: (LRU) <empty> (MRU)
            Assertions.assertFalse(evictedItem.isPresent());
        }

    }
}
