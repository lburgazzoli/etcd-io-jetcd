/*
 * Copyright 2016-2021 The jetcd authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.etcd.jetcd.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.etcd.jetcd.Maintenance;
import io.etcd.jetcd.api.SnapshotResponse;
import io.etcd.jetcd.grpc.GrpcService;
import io.etcd.jetcd.maintenance.AlarmResponse;
import io.etcd.jetcd.maintenance.DefragmentResponse;
import io.etcd.jetcd.maintenance.HashKVResponse;
import io.etcd.jetcd.maintenance.HashResponse;
import io.etcd.jetcd.maintenance.MoveLeaderResponse;
import io.etcd.jetcd.maintenance.StatusResponse;
import io.vertx.core.streams.ReadStream;

import static io.etcd.jetcd.common.Preconditions.checkArgument;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

/**
 * Implementation of maintenance client.
 */
final class MaintenanceClient extends AbstractClient implements Maintenance {
    private final io.etcd.jetcd.api.MaintenanceGrpcClient client;

    MaintenanceClient(GrpcService grpcService) {
        super(grpcService);

        client = io.etcd.jetcd.api.MaintenanceGrpcClient.create(
            grpcService.getAuthenticatedGrpcClient(),
            grpcService.getServiceResolver().getTarget());
    }

    @Override
    public CompletableFuture<AlarmResponse> listAlarms() {
        io.etcd.jetcd.api.AlarmRequest alarmRequest = io.etcd.jetcd.api.AlarmRequest.newBuilder()
            .setAlarm(io.etcd.jetcd.api.AlarmType.NONE)
            .setAction(io.etcd.jetcd.api.AlarmRequest.AlarmAction.GET)
            .setMemberID(0)
            .build();

        return completable(client.alarm(alarmRequest), AlarmResponse::new);
    }

    @Override
    public CompletableFuture<AlarmResponse> alarmDisarm(io.etcd.jetcd.maintenance.AlarmMember member) {
        checkArgument(member.memberId() != 0, "the member id can not be 0");
        checkArgument(member.alarmType() != io.etcd.jetcd.maintenance.AlarmType.NONE, "alarm type can not be NONE");

        io.etcd.jetcd.api.AlarmRequest alarmRequest = io.etcd.jetcd.api.AlarmRequest.newBuilder()
            .setAlarm(io.etcd.jetcd.api.AlarmType.NOSPACE)
            .setAction(io.etcd.jetcd.api.AlarmRequest.AlarmAction.DEACTIVATE)
            .setMemberID(member.memberId())
            .build();

        return completable(client.alarm(alarmRequest), AlarmResponse::new);
    }

    @Override
    public CompletableFuture<DefragmentResponse> defragmentMember(String target) {
        return completable(
            client.defragment(io.etcd.jetcd.api.DefragmentRequest.getDefaultInstance()),
            DefragmentResponse::new);
    }

    @Override
    public CompletableFuture<StatusResponse> statusMember(String target) {
        return completable(
            client.status(io.etcd.jetcd.api.StatusRequest.getDefaultInstance()),
            StatusResponse::new);
    }

    @Override
    public CompletableFuture<MoveLeaderResponse> moveLeader(long transfereeID) {
        return completable(
            client.moveLeader(io.etcd.jetcd.api.MoveLeaderRequest.newBuilder().setTargetID(transfereeID).build()),
            MoveLeaderResponse::new);
    }

    @Override
    public CompletableFuture<HashResponse> hash(String target) {
        return completable(
            client.hash(io.etcd.jetcd.api.HashRequest.getDefaultInstance()),
            HashResponse::new);
    }

    @Override
    public CompletableFuture<HashKVResponse> hashKV(String target, long rev) {
        return completable(
            client.hashKV(io.etcd.jetcd.api.HashKVRequest.newBuilder().setRevision(rev).build()),
            HashKVResponse::new);
    }

    @Override
    public CompletableFuture<Long> snapshot(OutputStream outputStream) {
        final CompletableFuture<Long> answer = new CompletableFuture<>();
        final AtomicLong bytes = new AtomicLong(0);

        client.snapshot(io.etcd.jetcd.api.SnapshotRequest.getDefaultInstance()).onComplete(ar -> {
            if (ar.failed()) {
                answer.completeExceptionally(toEtcdException(ar.cause()));
            } else {
                SerialExecutor serialExecutor = new SerialExecutor();
                ReadStream<SnapshotResponse> rs = ar.result();
                rs.pause();
                rs.handler(r -> {
                    serialExecutor.executeBlocking(() -> {
                        rs.pause();
                        try {
                            r.getBlob().writeTo(outputStream);
                            bytes.addAndGet(r.getBlob().size());
                        } catch (IOException e) {
                            answer.completeExceptionally(toEtcdException(e));
                        }
                    }).onComplete(v -> rs.resume());
                });
                rs.endHandler(event -> {
                    answer.complete(bytes.get());
                });
                rs.exceptionHandler(e -> {
                    answer.completeExceptionally(toEtcdException(e));
                });
                rs.resume();
            }
        });

        return answer;
    }

    @Override
    public void snapshot(Maintenance.Listener listener) {
        client.snapshot(io.etcd.jetcd.api.SnapshotRequest.getDefaultInstance()).onComplete(ar -> {
            if (ar.failed()) {
                listener.onError(toEtcdException(ar.cause()));
            } else {
                ar.result().handler(r -> listener.onNext(new io.etcd.jetcd.maintenance.SnapshotResponse(r)));
                ar.result().endHandler(event -> listener.onCompleted());
                ar.result().exceptionHandler(e -> listener.onError(toEtcdException(e)));
            }
        });
    }
}
