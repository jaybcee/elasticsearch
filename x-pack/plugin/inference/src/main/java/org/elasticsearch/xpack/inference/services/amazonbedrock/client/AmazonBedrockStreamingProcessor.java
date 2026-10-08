/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

abstract class AmazonBedrockStreamingProcessor<T> implements Flow.Processor<ConverseStreamOutput, T> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockStreamingProcessor.class);

    /**
     * The first error wins: once a stream fails, later errors (for example the SDK reporting the cancellation that followed the
     * failure) must not replace the one that is delivered downstream.
     */
    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    /**
     * Claimed by whichever path delivers downstream's terminal signal, so downstream sees at most one onError or onComplete.
     */
    private final AtomicBoolean downstreamTerminated = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    /**
     * The purpose of demand is solely to guard against the situation where the bedrock sdk can complete the future before the publisher
     * and subscriber aren't connected together via {@link #subscribe(Flow.Subscriber)} and {@link AmazonBedrockInferenceClient}
     * getAmazonBedrockStreamingProcessor(). We should refactor this logic to be like
     * {@link org.elasticsearch.xpack.inference.services.sagemaker.SageMakerClient} which tracks the subscriber and publisher via atomic
     * references instead of using a demand variable.
     */
    final AtomicLong demand = new AtomicLong(0);
    /**
     * Whether upstream has finished. This is not the same as downstream having received its terminal signal, see
     * {@link #downstreamTerminated}.
     */
    final AtomicBoolean isDone = new AtomicBoolean(false);

    volatile Flow.Subscription upstream;

    volatile Flow.Subscriber<? super T> downstream;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (upstream == null) {
            upstream = subscription;
            var currentRequestCount = demand.getAndUpdate(i -> 0);
            if (currentRequestCount > 0) {
                upstream.request(currentRequestCount);
            }
        } else {
            subscription.cancel();
        }
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        if (downstream == null) {
            downstream = subscriber;
            downstream.onSubscribe(new StreamSubscription());
        } else {
            subscriber.onError(new IllegalStateException("Subscriber already set."));
        }
    }

    /**
     * The original throwable is stored and delivered as is, rather than wrapped, so that callers which look for a specific exception
     * type (for example {@code UnifiedChatCompletionException.fromThrowable}, which only unwraps
     * {@link org.elasticsearch.ElasticsearchWrapperException}) still find it.
     */
    @Override
    public void onError(Throwable amazonBedrockRuntimeException) {
        ExceptionsHelper.maybeDieOnAnotherThread(amazonBedrockRuntimeException);
        error.compareAndSet(null, amazonBedrockRuntimeException);
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && downstreamTerminated.compareAndSet(false, true)) {
            var winner = error.get();
            runOnUtilityThreadPool(() -> downstream.onError(winner));
        }
    }

    private boolean checkAndResetDemand() {
        return demand.getAndUpdate(i -> 0L) > 0L;
    }

    @Override
    public void onComplete() {
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && downstreamTerminated.compareAndSet(false, true)) {
            downstream.onComplete();
        }
    }

    /**
     * Fails the stream because processing an upstream item failed. Unlike {@link #onError}, this does not wait for demand: demand was
     * reset when the failed item was forked, and downstream is still waiting for the response to its request. The error is recorded and
     * the terminal signal claimed before upstream is cancelled, so an error the SDK reports for the cancellation cannot replace this one.
     */
    void failStream(Throwable t) {
        error.compareAndSet(null, t);
        var claimed = downstreamTerminated.compareAndSet(false, true);
        if (upstream != null) {
            upstream.cancel();
        }
        if (claimed) {
            var winner = error.get();
            runOnUtilityThreadPool(() -> downstream.onError(winner));
        } else {
            logger.debug("Amazon Bedrock stream already terminated, dropping processing failure", t);
        }
    }

    boolean isDownstreamTerminated() {
        return downstreamTerminated.get();
    }

    protected AmazonBedrockStreamingProcessor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    void runOnUtilityThreadPool(Runnable runnable) {
        try {
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(runnable);
        } catch (Exception e) {
            logger.error(Strings.format("failed to fork [%s] to utility thread pool", runnable), e);
        }
    }

    /**
     * Records demand for {@code n} more items as if downstream had requested them. Subclasses use this when an event they reset demand
     * for produced nothing to send, so downstream's request is still outstanding. Once upstream has finished, this delivers the
     * terminal signal instead.
     */
    void requestOnBehalfOfDownstream(long n) {
        if (downstreamTerminated.get()) {
            return;
        }
        demand.updateAndGet(i -> {
            var sum = i + n;
            return sum >= 0 ? sum : Long.MAX_VALUE;
        });
        if (upstream == null) {
            // wait for upstream to subscribe before forwarding request
            return;
        }
        if (upstreamIsRunning()) {
            requestOnMlThread(n);
        } else if (downstreamTerminated.compareAndSet(false, true)) {
            var storedError = error.get();
            if (storedError != null) {
                downstream.onError(storedError);
            } else {
                downstream.onComplete();
            }
        }
    }

    private boolean upstreamIsRunning() {
        return isDone.get() == false && error.get() == null;
    }

    private void requestOnMlThread(long n) {
        var currentThreadPool = EsExecutors.executorName(Thread.currentThread());
        if (UTILITY_THREAD_POOL_NAME.equalsIgnoreCase(currentThreadPool)) {
            upstream.request(n);
        } else {
            runOnUtilityThreadPool(() -> upstream.request(n));
        }
    }

    class StreamSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
            if (n > 0L) {
                requestOnBehalfOfDownstream(n);
            } else if (downstreamTerminated.get() == false) {
                cancel();
                if (downstreamTerminated.compareAndSet(false, true)) {
                    downstream.onError(new IllegalStateException("Cannot request a negative number."));
                }
            }
        }

        @Override
        public void cancel() {
            if (upstream != null && upstreamIsRunning()) {
                upstream.cancel();
            }
        }
    }
}
