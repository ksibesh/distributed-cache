package com.example.cache.eviction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class LeastFrequentlyUsedStrategyTest {

    private LeastFrequentlyUsedStrategy<String> lfuStrategy;

    @BeforeEach
    public void setUp() {
        lfuStrategy = new LeastFrequentlyUsedStrategy<>();
    }

    @Test
    public void testEvictionAfterSinglePut() {
        String key = "test_key";
        lfuStrategy.onPut(key);

        Optional<String> evictionKey = lfuStrategy.evict();

        Assertions.assertTrue(evictionKey.isPresent());
        Assertions.assertEquals(key, evictionKey.get());
    }

    @Test
    public void testEvictionAfterMultiplePut() {
        String[] keys = new String[]{"test_key_1", "test_key_2", "test_key_3", "test_key_4"};
        lfuStrategy.onPut(keys[0]);
        lfuStrategy.onPut(keys[1]);
        lfuStrategy.onPut(keys[2]);
        lfuStrategy.onPut(keys[3]);

        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[0], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[1], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[2], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[3], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());
        }
    }

    @Test
    public void testEvictionAfterMultiplePutAndAccess() {
        String[] keys = new String[]{"test_key_0", "test_key_1", "test_key_2", "test_key_3"};
        lfuStrategy.onPut(keys[0]); // 0(1)
        lfuStrategy.onPut(keys[1]); // 0(1) -> 1(1)
        lfuStrategy.onAccess(keys[1]);  // 0(1) -> 1(2)
        lfuStrategy.onPut(keys[2]); // 0(1) -> 2(1) -> 1(2)
        lfuStrategy.onAccess(keys[0]);  // 2(1) -> 1(2) -> 0(2)
        lfuStrategy.onAccess(keys[1]);  // 2(1) -> 0(2) -> 1(3)
        lfuStrategy.onAccess(keys[0]);  // 2(1) -> 1(3) -> 0(3)
        lfuStrategy.onPut(keys[3]); // 2(1) -> 3(1) -> 1(3) -> 0(3)
        lfuStrategy.onAccess(keys[0]);  // 2(1) -> 3(1) -> 1(3) -> 0(4)
        lfuStrategy.onAccess(keys[3]);  // 2(1) -> 3(2) -> 1(3) -> 0(4)

        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[2], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // 3(2) -> 1(3) -> 0(4)
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[3], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // 1(3) -> 0(4)
        }

        lfuStrategy.onPut(keys[2]); // 2(1) -> 1(3) -> 0(4)
        lfuStrategy.onPut(keys[3]); // 2(1) -> 3(1) -> 1(3) -> 0(4)
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[2], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // 3(1) -> 1(3) -> 0(4)
        }

        lfuStrategy.onPut(keys[2]); // 3(1) -> 2(1) -> 1(3) -> 0(4)
        lfuStrategy.onPut(keys[2]); // 3(1) -> 2(2) -> 1(3) -> 0(4)
        lfuStrategy.onPut(keys[2]); // 3(1) -> 1(3) -> 2(3) -> 0(4)
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[3], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // 1(3) -> 2(3) -> 0(4)
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[1], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // 2(3) -> 0(4)
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[2], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // 0(4)
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isPresent());
            Assertions.assertEquals(keys[0], evictionKey.get());
            lfuStrategy.onRemove(evictionKey.get());    // <empty>
        }
        {
            Optional<String> evictionKey = lfuStrategy.evict();
            Assertions.assertTrue(evictionKey.isEmpty());
        }
    }
}
