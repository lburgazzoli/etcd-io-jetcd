package io.etcd.jetcd.impl;

import java.util.ArrayDeque;
import java.util.Queue;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Executes blocking tasks sequentially per gRPC stream.
 * Tasks should be submitted from the same Vert.x context.
 */
public class SerialExecutor {

    private final Queue<Runnable> queue = new ArrayDeque<>();
    private boolean processing = false;
    private Context context = null;

    /**
     * Submit a blocking task to be executed sequentially.
     * Returns a Future that completes when the task finishes.
     */
    public Future<Void> executeBlocking(Runnable task) {
        if (Vertx.currentContext() == null) {
            return Future
                .failedFuture(new IllegalStateException("SerialExecutor methods must be called from a Vert.x context"));
        }
        if (context == null) {
            context = Vertx.currentContext();
        } else if (context != Vertx.currentContext()) {
            return Future
                .failedFuture(new IllegalStateException("SerialExecutor methods must be called from the same Vert.x context"));
        }

        return Future.future(promise -> {
            queue.add(() -> context.executeBlocking(() -> {
                task.run();
                return null;
            }, false)

                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.tryComplete();
                    } else {
                        promise.tryFail(ar.cause());
                    }
                    processNext();
                }));

            if (!processing) {
                processing = true;
                processNext();
            }
        });
    }

    // Process the next task in the queue
    private void processNext() {
        Runnable nextTask = queue.poll();
        if (nextTask == null) {
            processing = false;
            return;
        }
        nextTask.run();
    }
}
