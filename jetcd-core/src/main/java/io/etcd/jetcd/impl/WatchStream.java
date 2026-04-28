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

import java.util.concurrent.atomic.AtomicReference;

import io.etcd.jetcd.api.WatchGrpcClient;
import io.etcd.jetcd.api.WatchRequest;
import io.etcd.jetcd.api.WatchResponse;
import io.etcd.jetcd.common.exception.Exceptions;
import io.etcd.jetcd.grpc.GrpcService;
import io.vertx.core.Future;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Encapsulates the lifecycle management of a gRPC watch stream.
 * Handles stream creation, message sending, and cleanup.
 *
 * <p>
 * All operations should be called from the Vert.x event loop thread
 * to ensure thread safety without explicit synchronization.
 */
final class WatchStream {

    /**
     * Callback interface for stream events.
     */
    interface Handler {
        /**
         * Called when a watch response is received.
         */
        void onMessage(WatchResponse response);

        /**
         * Called when the stream ends normally.
         */
        void onEnd();

        /**
         * Called when the stream encounters an error.
         */
        void onError(Throwable error);

        /**
         * Called when the write stream is ready to accept messages.
         * The initial WatchCreateRequest should be sent here.
         */
        void onWriteStreamReady(WatchStream stream);
    }

    private final GrpcService grpcService;
    private final AtomicReference<WriteStream<WatchRequest>> writeStreamRef;
    private final AtomicReference<ReadStream<WatchResponse>> readStreamRef;

    private WatchGrpcClient grpcClient;

    WatchStream(GrpcService grpcService) {
        this.grpcService = grpcService;
        this.writeStreamRef = new AtomicReference<>();
        this.readStreamRef = new AtomicReference<>();
    }

    /**
     * Connects to the watch stream asynchronously.
     * All handler callbacks will be invoked on the Vert.x event loop.
     *
     * @param  handler the handler to receive stream events
     * @return         a future that completes when the connection is established or fails
     */
    Future<Void> connect(Handler handler) {
        if (readStreamRef.get() != null) {
            return Future.succeededFuture();
        }

        grpcClient = createWatchClient();

        return grpcClient.watch((ws, err) -> {
            // TODO: Error will be handled by the Future's failure path
            if (err == null) {
                writeStreamRef.set(ws);
                handler.onWriteStreamReady(this);
            }
        }).map(rs -> {
            SerialExecutor serialExecutor = new SerialExecutor();
            rs.pause();
            rs.handler(response -> {
                rs.pause();
                serialExecutor.executeBlocking(() -> Exceptions.quietly(() -> handler.onMessage(response)))
                    .onComplete(v -> rs.resume());
            });
            rs.endHandler(v -> serialExecutor.executeBlocking(() -> Exceptions.quietly(handler::onEnd)));
            rs.exceptionHandler(t -> serialExecutor.executeBlocking(() -> Exceptions.quietly(() -> handler.onError(t))));
            rs.resume();
            readStreamRef.set(rs);
            return null;
        });
    }

    /**
     * Sends a request on the write stream.
     *
     * @param  request the request to send
     * @return         true if the request was sent, false if write stream not ready
     */
    boolean send(WatchRequest request) {
        WriteStream<WatchRequest> ws = writeStreamRef.get();
        if (ws == null) {
            return false;
        }

        Exceptions.quietly(() -> ws.write(request));
        return true;
    }

    /**
     * Disconnects and cleans up the stream resources.
     * Safe to call multiple times.
     */
    void disconnect() {
        WriteStream<WatchRequest> ws = writeStreamRef.getAndSet(null);
        ReadStream<WatchResponse> rs = readStreamRef.getAndSet(null);
        grpcClient = null;

        Exceptions.quietly(() -> {
            if (ws != null) {
                ws.end();
            }
        });

        Exceptions.quietly(() -> {
            if (rs != null) {
                rs.handler(null);
                rs.endHandler(null);
                rs.exceptionHandler(null);
            }
        });
    }

    /**
     * Returns whether the stream is fully connected (both read and write streams ready).
     */
    boolean isConnected() {
        return readStreamRef.get() != null && writeStreamRef.get() != null;
    }

    /**
     * Returns whether the write stream is ready to send messages.
     */
    boolean isWriteStreamReady() {
        return writeStreamRef.get() != null;
    }

    private WatchGrpcClient createWatchClient() {
        return WatchGrpcClient.create(
            grpcService.getAuthenticatedGrpcClient(),
            grpcService.getServiceResolver().getTarget());
    }
}
