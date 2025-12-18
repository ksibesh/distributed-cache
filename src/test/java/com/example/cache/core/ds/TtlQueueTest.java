package com.example.cache.core.ds;

import com.example.cache.util.SystemUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

public class TtlQueueTest {

    private TtlQueue<String> ttlQueue;

    @BeforeEach
    public void setup() {
        ttlQueue = new TtlQueue<>();
    }

    @Test
    public void testEmptyQueue() {
        Assertions.assertTrue(ttlQueue.isEmpty());
        Assertions.assertEquals(0, ttlQueue.size());
    }

    @Test
    public void testAddSingleEntry() {
        long ttlInSec = SystemUtil.getCurrentTimeInSec();
        String key = "testKey";

        ttlQueue.add(ttlInSec, key);

        Assertions.assertFalse(ttlQueue.isEmpty());
        Assertions.assertEquals(1, ttlQueue.size());
        Assertions.assertTrue(ttlQueue.peek().isPresent());
        Assertions.assertEquals(ttlInSec, ttlQueue.peek().get());
    }

    @Test
    public void testAddMultipleEntries() {
        long minTtl = Long.MAX_VALUE;

        for (int i = 0; i < 10; i++) {
            long ttlInSec = SystemUtil.getCurrentTimeInSec() + i;
            String key = "testKey" + i;
            ttlQueue.add(ttlInSec, key);

            minTtl = Math.min(minTtl, ttlInSec);
        }

        Assertions.assertFalse(ttlQueue.isEmpty());
        Assertions.assertEquals(10, ttlQueue.size());
        Assertions.assertTrue(ttlQueue.peek().isPresent());
        Assertions.assertEquals(minTtl, ttlQueue.peek().get());
    }

    @Test
    public void testAddWithSameKeyRefreshTtl() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();
        long[] ttls = new long[5];

        {
            String key = "testKey1";
            long ttlInSec = currentTimeInSec + 1;
            ttlQueue.add(ttlInSec, key);
            Assertions.assertTrue(ttlQueue.peek().isPresent());
            Assertions.assertEquals(ttlInSec, ttlQueue.peek().get());
            ttls[0] = ttlInSec;
        }
        {
            String key = "testKey2";
            long ttlInSec = currentTimeInSec - 1;
            ttlQueue.add(ttlInSec, key);
            Assertions.assertTrue(ttlQueue.peek().isPresent());
            Assertions.assertEquals(ttlInSec, ttlQueue.peek().get());
            ttls[1] = ttlInSec;
        }
        {
            String key = "testKey3";
            long ttlInSec = currentTimeInSec + 2;
            ttlQueue.add(ttlInSec, key);
            Assertions.assertTrue(ttlQueue.peek().isPresent());
            Assertions.assertEquals(ttls[1], ttlQueue.peek().get());
            ttls[2] = ttlInSec;
        }
        {
            // repeated the key over here, with updated ttl, now the lowest ttl is not current-1, but current+1.
            // Assertions in this block proof that ttlQueue refresh the ttl valur for same key.
            String key = "testKey2";
            long ttlInSec = currentTimeInSec + 3;
            ttlQueue.add(ttlInSec, key);
            Assertions.assertTrue(ttlQueue.peek().isPresent());
            Assertions.assertEquals(ttls[0], ttlQueue.peek().get());
            ttls[3] = ttlInSec;
        }
    }

    @Test
    public void testPollWithSingleKeyForGivenTtl() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();

        String[] keys = new String[]{"testKey1", "testKey2", "testKey3", "testKey4", "testKey5"};
        long[] ttls = new long[]{currentTimeInSec, currentTimeInSec + 1, currentTimeInSec + 2, currentTimeInSec + 3, currentTimeInSec + 4};

        for (int i = 0; i < keys.length; i++) {
            ttlQueue.add(ttls[i], keys[i]);
        }

        Assertions.assertEquals(keys.length, ttlQueue.size());
        Assertions.assertTrue(ttlQueue.peek().isPresent());
        Assertions.assertEquals(ttls[0], ttlQueue.peek().get());

        Optional<Set<String>> expiredKeys = ttlQueue.poll();
        Assertions.assertTrue(expiredKeys.isPresent());
        Assertions.assertEquals(1, expiredKeys.get().size());
        Assertions.assertEquals(keys[0], expiredKeys.get().stream().findFirst().get());
        Assertions.assertEquals(keys.length - 1, ttlQueue.size());
    }

    @Test
    public void testPollWithMultipleKeysForGivenTtl() {
        long currentTimeInSec = SystemUtil.getCurrentTimeInSec();

        String[] keys = new String[]{"testKey1", "testKey2", "testKey3", "testKey4", "testKey5"};
        long[] ttls = new long[]{currentTimeInSec, currentTimeInSec, currentTimeInSec, currentTimeInSec + 3, currentTimeInSec + 4};

        for (int i = 0; i < keys.length; i++) {
            ttlQueue.add(ttls[i], keys[i]);
        }

        Assertions.assertEquals(3, ttlQueue.size());
        Assertions.assertTrue(ttlQueue.peek().isPresent());
        Assertions.assertEquals(ttls[0], ttlQueue.peek().get());

        Optional<Set<String>> expiredKeys = ttlQueue.poll();
        Assertions.assertTrue(expiredKeys.isPresent());
        Assertions.assertEquals(3, expiredKeys.get().size());
        Assertions.assertEquals(2, ttlQueue.size());
    }

}
