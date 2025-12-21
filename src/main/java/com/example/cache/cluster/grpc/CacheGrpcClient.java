package com.example.cache.cluster.grpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CacheGrpcClient {

    private final Map<String, CacheServiceGrpc.CacheServiceFutureStub> stubs = new HashMap<>();
    private final Map<String, ManagedChannel> channels = new HashMap<>();

    public void forwardGet(String address, String key, CompletableFuture<String> future) {
        try {
            GetRequest request = GetRequest.newBuilder().setKey(key).build();
            ListenableFuture<GetResponse> grpcFuture = getStub(address).get(request);

            Futures.addCallback(grpcFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(GetResponse response) {
                    future.complete(response.getFound() ? response.getValue() : null);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Async forward GET failed for address {}: {}", address, t.getMessage(), t);
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            log.error("Async forward GET failed for address {}: {}", address, e.getMessage(), e);
            future.completeExceptionally(e);
        }
    }

    public void forwardPut(String address, String key, String value, long ttlInSec,
                                              CompletableFuture<String> future) {

        try {
            PutRequest request = PutRequest.newBuilder().setKey(key).setValue(value).setTtlInSec(ttlInSec).build();
            ListenableFuture<PutResponse> grpcFuture = getStub(address).put(request);

            Futures.addCallback(grpcFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(PutResponse response) {
                    future.complete(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Async forward PUT failed for address {}: {}", address, t.getMessage(), t);
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            log.error("Async forward PUT failed for address {}: {}", address, e.getMessage(), e);
            future.completeExceptionally(e);
        }
    }

    public void forwardDelete(String address, String key, CompletableFuture<String> future) {
        try {
            DeleteRequest deleteRequest = DeleteRequest.newBuilder().setKey(key).build();
            ListenableFuture<DeleteResponse> grpcFuture = getStub(address).delete(deleteRequest);

            Futures.addCallback(grpcFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(DeleteResponse result) {
                    future.complete(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Async forward DELETE failed for address {}: {}", address, t.getMessage(), t);
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            log.error("Async forward DELETE failed for address {}: {}", address, e.getMessage(), e);
            future.completeExceptionally(e);
        }
    }

    private CacheServiceGrpc.CacheServiceFutureStub getStub(String address) {
        return stubs.computeIfAbsent(address, addr -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
            channels.put(addr, channel);
            return CacheServiceGrpc.newFutureStub(channel);
        });
    }

    public void shutdown() {
        channels.values().forEach(channel -> {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
