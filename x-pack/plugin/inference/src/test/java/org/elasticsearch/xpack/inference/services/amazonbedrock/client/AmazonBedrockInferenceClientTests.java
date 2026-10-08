/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests.createChatCompletionModel;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AmazonBedrockInferenceClientTests extends ESTestCase {

    public void testStreamFailureDeliversTheBedrockError() {
        var model = createChatCompletionModel("id", "region", "model", AmazonBedrockProvider.ANTHROPIC, "access", "secret");
        var client = AmazonBedrockMockInferenceClient.create(model, null);
        var publisher = client.converseUnifiedStream(ConverseStreamRequest.builder().build(), model);

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        publisher.subscribe(downstream);
        var subscription = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstream).onSubscribe(subscription.capture());
        subscription.getValue().request(1);

        var expectedError = BedrockRuntimeException.builder().message("throttled").build();
        client.converseStreamFuture().completeExceptionally(new CompletionException(expectedError));

        verify(downstream).onError(same(expectedError));
    }
}
