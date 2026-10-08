/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDelta;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDeltaEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingCompletionResults;

import java.util.ArrayDeque;

class AmazonBedrockCompletionStreamingProcessor extends AmazonBedrockStreamingProcessor<StreamingCompletionResults.Results> {
    protected AmazonBedrockCompletionStreamingProcessor(ThreadPool threadPool) {
        super(threadPool);
    }

    @Override
    public void onNext(ConverseStreamOutput item) {
        if (item.sdkEventType() == ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA) {
            item.accept(ConverseStreamResponseHandler.Visitor.builder().onContentBlockDelta(this::handleContentBlockDelta).build());
        } else {
            upstream.request(1);
        }
    }

    private void handleContentBlockDelta(ContentBlockDeltaEvent event) {
        // completion only streams text, so other deltas such as reasoning are skipped while keeping downstream's demand
        if (event.delta().type() != ContentBlockDelta.Type.TEXT) {
            upstream.request(1);
            return;
        }
        demand.set(0); // reset demand before we fork to another thread
        sendDownstreamOnAnotherThread(event);
    }

    // this is always called from a netty thread maintained by the AWS SDK, we'll move it to our thread to process the response
    private void sendDownstreamOnAnotherThread(ContentBlockDeltaEvent event) {
        runOnUtilityThreadPool(() -> {
            var text = event.delta().text();
            var result = new ArrayDeque<StreamingCompletionResults.Result>(1);
            result.offer(new StreamingCompletionResults.Result(text));
            var results = new StreamingCompletionResults.Results(result);
            downstream.onNext(results);
        });
    }
}
