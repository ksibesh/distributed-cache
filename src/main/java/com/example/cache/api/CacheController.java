package com.example.cache.api;

import com.example.cache.api.domain.GetResponse;
import com.example.cache.api.domain.PutRequest;
import com.example.cache.api.domain.PutResponse;
import com.example.cache.api.domain.DeleteResponse;
import com.example.cache.core.IDistributedCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;

import static com.example.cache.util.SystemUtil.DEFAULT_ERROR_CODE;

/**
 * As first draft we are using JSON serialization as default, we can make it configurable in the future.
 * TODO: Implemented default methods, batch operations are still missing, maybe in v2
 * TODO: Fix the error code, right now only DEFAULT error code is getting mapped everywhere.
 * TODO: System internal exception handling is missing, no distinction between system exception and java exception.
 */
@Slf4j
@RestController
@RequestMapping("/cache")
public class CacheController {

    private final IDistributedCache cacheCore;
    private final long timeout;

    @Autowired
    public CacheController(IDistributedCache cacheCore) {
        this.cacheCore = cacheCore;
        this.timeout = 500; // TODO: make this configurable
    }

    @RequestMapping(method = RequestMethod.PUT)
    public DeferredResult<PutResponse> put(@RequestBody PutRequest request) {
        DeferredResult<PutResponse> response = new DeferredResult<>(timeout, new PutResponse() {{
            setErrorCode("TIMEOUT");
        }});

        cacheCore.submitPut(request.getKey(), request.getValue(), request.getTtlInSec())
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Error putting key={}", request.getKey(), throwable);
                        response.setErrorResult(new PutResponse() {{
                            setErrorCode(DEFAULT_ERROR_CODE);
                            setErrorMessage(throwable.getMessage());
                        }});
                    } else {
                        response.setResult(new PutResponse() {{
                            setPutStatus(true);
                        }});
                    }
                });
        return response;
    }

    @RequestMapping(method = RequestMethod.GET)
    public DeferredResult<GetResponse> get(@RequestParam String key) {
        DeferredResult<GetResponse> response = new DeferredResult<>(timeout, new GetResponse() {{
            setErrorCode("TIMEOUT");
        }});

        cacheCore.submitGet(key)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Error getting key={}", key, throwable);
                        response.setErrorResult(new GetResponse() {{
                            setErrorCode(DEFAULT_ERROR_CODE);
                            setErrorMessage(throwable.getMessage());
                        }});
                    } else {
                        response.setResult(new GetResponse() {{
                            setValue(result);
                        }});
                    }
                });
        return response;
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public DeferredResult<DeleteResponse> remove(@RequestParam String key) {
        DeferredResult<DeleteResponse> response = new DeferredResult<>(timeout, new DeleteResponse() {{
            setErrorCode("TIMEOUT");
        }});

        cacheCore.submitDelete(key)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Error deleting key={}", key, throwable);
                        response.setErrorResult(new DeleteResponse() {{
                            setErrorCode(DEFAULT_ERROR_CODE);
                            setErrorMessage(throwable.getMessage());
                        }});
                    } else {
                        response.setResult(new DeleteResponse() {{
                            setRemoveStatus(true);
                        }});
                    }
                });
        return response;
    }
}
