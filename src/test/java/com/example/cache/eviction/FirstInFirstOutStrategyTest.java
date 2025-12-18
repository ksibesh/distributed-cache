package com.example.cache.eviction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class FirstInFirstOutStrategyTest {

    private FirstInFirstOutStrategy<String> fifoStrategy;

    @BeforeEach
    public void setUp() {
        fifoStrategy = new FirstInFirstOutStrategy<>();
    }

    @Test
    public void testEvictWithEmptyCache() {
        Optional<String> evictedItem = fifoStrategy.evict();
        Assertions.assertTrue(evictedItem.isEmpty());
    }

    @Test
    public void testTwoEvictsWithSingleElementInCache() {
        String[] keys = new String[]{"testKey1", "testKey2"};

        {
            fifoStrategy.onPut(keys[0]);
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[0], evictedItem.get());

            fifoStrategy.onRemove(evictedItem.get());
        }
        {
            Optional<String> evictedItems = fifoStrategy.evict();
            Assertions.assertTrue(evictedItems.isEmpty());
        }
    }

    @Test
    public void testMultipleEvictsWithMultipleInLineInsertsWithEndStateNotEmpty() {
        String[] keys = new String[]{"testKey1", "testKey2", "testKey3", "testKey4", "testKey5"};

        fifoStrategy.onPut(keys[0]);
        fifoStrategy.onPut(keys[1]);

        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[0], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[1], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isEmpty());
        }

        fifoStrategy.onPut(keys[2]);
        fifoStrategy.onPut(keys[1]);
        fifoStrategy.onPut(keys[3]);
        fifoStrategy.onPut(keys[4]);

        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[2], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[1], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
    }

    @Test
    public void testMultipleInserts() {
        String[] keys = new String[] {"testKey1", "testKey2", "testKey3"};

        fifoStrategy.onPut(keys[0]);
        fifoStrategy.onPut(keys[1]);
        fifoStrategy.onPut(keys[0]);
        fifoStrategy.onPut(keys[2]);

        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[1], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[0], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
        {
            Optional<String> evictedItem = fifoStrategy.evict();
            Assertions.assertTrue(evictedItem.isPresent());
            Assertions.assertEquals(keys[2], evictedItem.get());
            fifoStrategy.onRemove(evictedItem.get());
        }
    }

}
