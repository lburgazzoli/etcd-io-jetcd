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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.etcd.jetcd.Lease;
import io.etcd.jetcd.common.Service;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.grpc.GrpcService;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.support.CloseableClient;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newClosedLeaseClientException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.newEtcdException;
import static io.etcd.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of lease client.
 */
final class LeaseClient extends AbstractClient implements Lease {

    private static final int DEFAULT_FIRST_KEEPALIVE_TIMEOUT_MS = 5000;

    private final io.etcd.jetcd.api.LeaseGrpcClient client;
    private final Map<Long, KeepAliveObserver> keepAlives;
    private final KeepAlive keepAlive;
    private final DeadLine deadLine;
    private volatile boolean closed;

    LeaseClient(GrpcService grpcService) {
        super(grpcService);

        this.client = io.etcd.jetcd.api.LeaseGrpcClient.create(
            grpcService.getAuthenticatedGrpcClient(),
            grpcService.getServiceResolver().getTarget());
        this.keepAlives = new ConcurrentHashMap<>();
        this.keepAlive = new KeepAlive();
        this.deadLine = new DeadLine();
    }

    @Override
    public CompletableFuture<LeaseGrantResponse> grant(long ttl) {
        return execute(
            () -> client.leaseGrant(
                io.etcd.jetcd.api.LeaseGrantRequest.newBuilder()
                    .setTTL(ttl)
                    .build()),
            responseFactory::newLeaseGrantResponse,
            true);
    }

    @Override
    public CompletableFuture<LeaseGrantResponse> grant(long ttl, long timeout, TimeUnit unit) {
        return grant(ttl);
    }

    @Override
    public CompletableFuture<LeaseRevokeResponse> revoke(long leaseId) {
        return execute(
            () -> client.leaseRevoke(
                io.etcd.jetcd.api.LeaseRevokeRequest.newBuilder()
                    .setID(leaseId)
                    .build()),
            responseFactory::newLeaseRevokeResponse,
            true);
    }

    @Override
    public CompletableFuture<LeaseTimeToLiveResponse> timeToLive(long leaseId, LeaseOption option) {
        requireNonNull(option, "LeaseOption should not be null");

        io.etcd.jetcd.api.LeaseTimeToLiveRequest leaseTimeToLiveRequest = io.etcd.jetcd.api.LeaseTimeToLiveRequest.newBuilder()
            .setID(leaseId)
            .setKeys(option.attachedKeys())
            .build();

        return execute(
            () -> client.leaseTimeToLive(leaseTimeToLiveRequest),
            responseFactory::newLeaseTimeToLiveResponse,
            true);
    }

    @Override
    public synchronized CloseableClient keepAlive(long leaseId, Lease.Listener listener) {
        if (this.closed) {
            throw newClosedLeaseClientException();
        }

        KeepAliveObserver keepAlive = this.keepAlives.computeIfAbsent(leaseId, KeepAliveObserver::new);
        keepAlive.addListener(listener);

        this.keepAlive.start();
        this.deadLine.start();

        return new CloseableClient() {
            @Override
            public void close() {
                keepAlive.removeListener(listener);
            }
        };
    }

    @Override
    public CompletableFuture<LeaseKeepAliveResponse> keepAliveOnce(long leaseId) {
        final AtomicReference<WriteStream<io.etcd.jetcd.api.LeaseKeepAliveRequest>> writeStreamRef = new AtomicReference<>();
        final AtomicReference<ReadStream<io.etcd.jetcd.api.LeaseKeepAliveResponse>> readStreamRef = new AtomicReference<>();
        final CompletableFuture<LeaseKeepAliveResponse> future = new CompletableFuture<>();
        final io.etcd.jetcd.api.LeaseKeepAliveRequest req = io.etcd.jetcd.api.LeaseKeepAliveRequest.newBuilder().setID(leaseId)
            .build();

        client.leaseKeepAlive((writeStream, err) -> {
            if (err != null) {
                future.completeExceptionally(err);
            } else {
                writeStreamRef.set(writeStream);
                writeStream.write(req);
            }
        }).onComplete(ar -> {
            if (ar.failed()) {
                future.completeExceptionally(ar.cause());
            } else {
                ReadStream<io.etcd.jetcd.api.LeaseKeepAliveResponse> readStream = ar.result();
                readStreamRef.set(readStream);

                readStream.handler(r -> {
                    try {
                        if (r.getTTL() != 0) {
                            future.complete(new LeaseKeepAliveResponse(r));
                        } else {
                            future.completeExceptionally(
                                newEtcdException(ErrorCode.NOT_FOUND, "etcdserver: requested lease not found"));
                        }
                    } finally {
                        cleanupKeepAliveOnce(readStreamRef.get(), writeStreamRef.get());
                    }
                });

                readStream.exceptionHandler(t -> {
                    try {
                        future.completeExceptionally(t);
                    } finally {
                        cleanupKeepAliveOnce(readStreamRef.get(), writeStreamRef.get());
                    }
                });
            }
        });

        return future;
    }

    private void cleanupKeepAliveOnce(
        ReadStream<io.etcd.jetcd.api.LeaseKeepAliveResponse> readStream,
        WriteStream<io.etcd.jetcd.api.LeaseKeepAliveRequest> writeStream) {

        if (readStream != null) {
            readStream.handler(null);
            readStream.exceptionHandler(null);
            readStream.endHandler(null);
        }

        if (writeStream != null) {
            writeStream.end();
        }
    }

    @Override
    public synchronized void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;

        this.keepAlive.close();
        this.deadLine.close();

        final Throwable errResp = newClosedLeaseClientException();

        this.keepAlives.values().forEach(v -> v.onError(errResp));
        this.keepAlives.clear();
    }

    private final class KeepAlive extends Service {
        private volatile Long task;
        private volatile Long restart;
        private volatile WriteStream<io.etcd.jetcd.api.LeaseKeepAliveRequest> requestStream;

        KeepAlive() {
        }

        @Override
        public void doStart() {
            client.leaseKeepAlive((writeStream, err) -> {
                if (err != null) {
                    Vertx.currentContext().executeBlocking(() -> {
                        handleException(err);
                        return null;
                    }, false);
                } else {
                    writeHandler(writeStream);
                }
            }).onComplete(ar -> {
                if (ar.failed()) {
                    Vertx.currentContext().executeBlocking(() -> {
                        handleException(ar.cause());
                        return null;
                    }, false);
                } else {
                    SerialExecutor serialExecutor = new SerialExecutor();
                    ReadStream<io.etcd.jetcd.api.LeaseKeepAliveResponse> readStream = ar.result();
                    readStream.pause();
                    readStream.handler(response -> serialExecutor.executeBlocking(() -> handleResponse(response))
                        .onComplete(v -> readStream.resume()));
                    readStream.exceptionHandler(t -> serialExecutor.executeBlocking(() -> handleException(t)));
                    readStream.resume();
                }
            });
        }

        @Override
        public void doStop() {
            if (requestStream != null) {
                requestStream.end();
            }
            if (this.restart != null) {
                grpc().vertx().cancelTimer(this.restart);
            }
            if (this.task != null) {
                grpc().vertx().cancelTimer(this.task);
            }
        }

        @Override
        public void close() {
            super.close();

            this.task = null;
            this.restart = null;
        }

        private void writeHandler(WriteStream<io.etcd.jetcd.api.LeaseKeepAliveRequest> stream) {
            requestStream = stream;

            task = grpc().vertx().setPeriodic(
                0,
                500,
                l -> {
                    keepAlives.values().forEach(element -> sendKeepAlive(element, stream));
                });
        }

        private void sendKeepAlive(KeepAliveObserver observer, WriteStream<io.etcd.jetcd.api.LeaseKeepAliveRequest> stream) {
            if (observer.getNextKeepAlive() < System.currentTimeMillis()) {
                stream.write(
                    io.etcd.jetcd.api.LeaseKeepAliveRequest.newBuilder().setID(observer.getLeaseId()).build());
            }
        }

        private synchronized void handleResponse(io.etcd.jetcd.api.LeaseKeepAliveResponse leaseKeepAliveResponse) {
            if (!this.isRunning()) {
                return;
            }

            final long leaseID = leaseKeepAliveResponse.getID();
            final long ttl = leaseKeepAliveResponse.getTTL();
            final KeepAliveObserver ka = keepAlives.get(leaseID);

            if (ka == null) {
                return;
            }

            if (ttl > 0) {
                long nextKeepAlive = System.currentTimeMillis() + ttl * 1000 / 3;
                ka.setNextKeepAlive(nextKeepAlive);
                ka.setDeadLine(System.currentTimeMillis() + ttl * 1000);
                ka.onNext(leaseKeepAliveResponse);
            } else {
                keepAlives.remove(leaseID);
                ka.onError(newEtcdException(ErrorCode.NOT_FOUND, "etcdserver: requested lease not found"));
            }
        }

        private synchronized void handleException(Throwable throwable) {
            if (!this.isRunning()) {
                return;
            }

            keepAlives.values().forEach(ka -> ka.onError(throwable));

            restart = grpc().vertx().setTimer(
                500,
                l -> {
                    if (isRunning()) {
                        restart();
                    }
                });
        }
    }

    private class DeadLine extends Service {
        private volatile Long task;

        DeadLine() {
        }

        @Override
        public void doStart() {
            this.task = grpc().vertx().setPeriodic(
                0,
                1000,
                l -> {
                    long now = System.currentTimeMillis();

                    keepAlives.values().removeIf(ka -> {
                        if (ka.getDeadLine() < now) {
                            Vertx.currentContext().executeBlocking(() -> {
                                ka.onCompleted();
                                return null;
                            }, false);
                            return true;
                        }
                        return false;
                    });
                });
        }

        @Override
        public void doStop() {
            if (this.task != null) {
                grpc().vertx().cancelTimer(this.task);
            }
        }
    }

    private final class KeepAliveObserver {
        private final List<Lease.Listener> listeners;
        private final long leaseId;

        private long deadLine;
        private long nextKeepAlive;

        KeepAliveObserver(long leaseId) {
            this(leaseId, Collections.emptyList());
        }

        KeepAliveObserver(long leaseId, Collection<Lease.Listener> listeners) {
            this.nextKeepAlive = System.currentTimeMillis();

            int initialKeepAliveTimeoutMs = grpc().builder().keepaliveTimeout() != null
                ? Math.toIntExact(grpc().builder().keepaliveTimeout().toMillis())
                : DEFAULT_FIRST_KEEPALIVE_TIMEOUT_MS;
            this.deadLine = nextKeepAlive + initialKeepAliveTimeoutMs;

            this.listeners = new CopyOnWriteArrayList<>(listeners);
            this.leaseId = leaseId;
        }

        long getLeaseId() {
            return leaseId;
        }

        long getDeadLine() {
            return deadLine;
        }

        void setDeadLine(long deadLine) {
            this.deadLine = deadLine;
        }

        void addListener(Lease.Listener listener) {
            this.listeners.add(listener);
        }

        void removeListener(Lease.Listener listener) {
            this.listeners.remove(listener);

            if (this.listeners.isEmpty()) {
                keepAlives.remove(leaseId);
            }
        }

        long getNextKeepAlive() {
            return nextKeepAlive;
        }

        void setNextKeepAlive(long nextKeepAlive) {
            this.nextKeepAlive = nextKeepAlive;
        }

        void onNext(io.etcd.jetcd.api.LeaseKeepAliveResponse response) {
            for (Lease.Listener listener : listeners) {
                listener.onNext(new LeaseKeepAliveResponse(response));
            }
        }

        void onError(Throwable throwable) {
            for (Lease.Listener listener : listeners) {
                listener.onError(toEtcdException(throwable));
            }
        }

        void onCompleted() {
            this.listeners.forEach(Lease.Listener::onCompleted);
            this.listeners.clear();
        }
    }
}
