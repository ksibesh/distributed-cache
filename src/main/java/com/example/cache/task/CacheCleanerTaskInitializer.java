package com.example.cache.task;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CacheCleanerTaskInitializer {
    private final CacheCleanerTask<?, ?> cacheCleanerTask;
    private final int cleanerThreadPool;

    private ExecutorService cacheCleanerExecutor;

    public CacheCleanerTaskInitializer(CacheCleanerTask<?, ?> cacheCleanerTask, int cleanerThreadPool) {
        this.cacheCleanerTask = cacheCleanerTask;
        this.cleanerThreadPool = cleanerThreadPool;
    }

    public void init() {
        log.info("[CacheCleanerTaskInitializer.Initializing] [threadPoolSize={}]", cleanerThreadPool);
        cacheCleanerExecutor = Executors.newFixedThreadPool(cleanerThreadPool, r -> {
            Thread t = new Thread(r);
            t.setName("Cache-Cleaner-Thread-" + t.threadId());
            t.setDaemon(true);
            return t;
        });

        cacheCleanerExecutor.submit(cacheCleanerTask);
    }

    public void destroy() {
        log.info("[CacheCleanerTaskInitializer.Stop]");
        cacheCleanerTask.stop();
        cacheCleanerExecutor.shutdown();

        try {
            if(!cacheCleanerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("[CacheCleanerTaskInitializer.Stop.ForceStop] " +
                        "[msg=Threads did not stop gracefully within timeout (5s). Attempting forced shutdown.]");
                cacheCleanerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("[CacheCleanerTaskInitializer.Stop.Interrupted.ForceStop] " +
                    "[msg=Interrupted while waiting for cleaner thread to stop. Forcing immediate shutdown.]");
            cacheCleanerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
