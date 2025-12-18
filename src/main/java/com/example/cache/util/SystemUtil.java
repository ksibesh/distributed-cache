package com.example.cache.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public final class SystemUtil {

    public static final String DEFAULT_ERROR_CODE = "ERROR.DEFAULT";

    private SystemUtil() {
        // no-op
    }

    public static long getCurrentTimeInSec() {
        return LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    }
}
