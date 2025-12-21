package com.example.cache.cluster.grpc;

import com.example.cache.core.IDistributedCache;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
public class CacheGrpcService extends CacheServiceGrpc.CacheServiceImplBase {

    private final IDistributedCache localCache;

    public CacheGrpcService(IDistributedCache localCache) {
        this.localCache = localCache;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        localCache.submitGet(request.getKey())
                .handle((res, ex) -> {
                    if (ex != null) {
                        log.error("Error during remote gRPC GET", ex);
                        responseObserver.onError(ex);
                    } else {
                        responseObserver.onNext(GetResponse.newBuilder()
                                .setValue(res != null ? res : "")
                                .setFound(res != null)
                                .build());
                        responseObserver.onCompleted();
                    }
                    return null;
                });
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        localCache.submitPut(request.getKey(), request.getValue(), request.getTtlInSec())
                .handle((res, ex) -> {
                    if (ex != null) {
                        log.error("Error during remote gRPC PUT", ex);
                        responseObserver.onError(ex);
                    } else {
                        responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                    }
                    return null;
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        localCache.submitDelete(request.getKey())
                .handle((res, ex) -> {
                    if (ex != null) {
                        log.error("Error during remote gRPC DELETE", ex);
                        responseObserver.onError(ex);
                    } else {
                        responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
                        responseObserver.onCompleted();
                    }
                    return null;
                });
    }
}
