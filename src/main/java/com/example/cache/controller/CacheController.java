package com.example.cache.controller;

import com.example.cache.controller.domain.GetResponse;
import com.example.cache.controller.domain.PutRequest;
import com.example.cache.controller.domain.PutResponse;
import com.example.cache.controller.domain.RemoveResponse;
import com.example.cache.core.IDistributedCache;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.example.cache.util.SystemUtil.DEFAULT_ERROR_CODE;

/**
 * As first draft we are using JSON serialization as default, we can make it configurable in the future.
 * TODO: Implemented default methods, batch operations are still missing, maybe in v2
 * TODO: Fix the error code, right now only DEFAULT error code is getting mapped everywhere.
 * TODO: System internal exception handling is missing, no distinction between system exception and java exception.
 */
@RestController
@RequestMapping("/cache")
public class CacheController {

    private final IDistributedCache<String, String> distributedCache;

    public CacheController(IDistributedCache<String, String> distributedCache) {
        this.distributedCache = distributedCache;
    }

    @RequestMapping(method = RequestMethod.PUT)
    public ResponseEntity<PutResponse> put(@RequestBody PutRequest request) {
        try {
            distributedCache.put(request.getKey(), request.getValue(), request.getTtlInSec());
            PutResponse response = new PutResponse();
            response.setPutStatus(true);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            PutResponse response = new PutResponse();
            response.setErrorCode(DEFAULT_ERROR_CODE);
            response.setErrorMessage(e.getMessage());
            return ResponseEntity.internalServerError().body(response);

        }
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<GetResponse> get(@RequestParam String key) {
        try {
            String value = distributedCache.get(key);
            GetResponse response = new GetResponse();
            response.setValue(value);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            GetResponse response = new GetResponse();
            response.setErrorCode(DEFAULT_ERROR_CODE);
            response.setErrorMessage(e.getMessage());
            return ResponseEntity.internalServerError().body(response);

        }
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public ResponseEntity<RemoveResponse> remove(@RequestParam String key) {
        try {
            distributedCache.delete(key);
            RemoveResponse response = new RemoveResponse();
            response.setRemoveStatus(true);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            RemoveResponse response = new RemoveResponse();
            response.setErrorCode(DEFAULT_ERROR_CODE);
            response.setErrorMessage(e.getMessage());
            return ResponseEntity.internalServerError().body(response);

        }
    }
}
