/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.fg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huaweicloud.sdk.core.auth.BasicCredentials;
import com.huaweicloud.sdk.core.http.HttpConfig;
import com.huaweicloud.sdk.functiongraph.v2.FunctionGraphAsyncClient;
import com.huaweicloud.sdk.functiongraph.v2.model.AsyncInvokeFunctionRequest;
import com.huaweicloud.sdk.functiongraph.v2.model.AsyncInvokeFunctionResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author hezhangjian
 */
@Slf4j
public class FunctionGraphSink implements Sink<byte[]> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
    };

    private FunctionGraphAsyncClient functionGraphAsyncClient;

    private String functionUrn;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        final FunctionGraphConfig functionGraphConfig = FunctionGraphConfig.load(config);
        final BasicCredentials credentials = new BasicCredentials()
                .withAk(functionGraphConfig.getAk())
                .withSk(functionGraphConfig.getSk())
                .withProjectId(functionGraphConfig.getProjectId());
        HttpConfig httpConfig = HttpConfig.getDefaultHttpConfig();
        httpConfig.withIgnoreSSLVerification(true);
        functionGraphAsyncClient = FunctionGraphAsyncClient.newBuilder()
                .withHttpConfig(httpConfig)
                .withCredential(credentials)
                .withEndpoint(functionGraphConfig.getFunctionGraphEndpoint())
                .build();
        functionUrn = functionGraphConfig.getFunctionUrn();
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        if (!record.getMessage().isPresent()) {
            record.ack();
            return;
        }
        Message<byte[]> message = record.getMessage().get();
        final AsyncInvokeFunctionRequest invokeFunctionRequest = new AsyncInvokeFunctionRequest();
        invokeFunctionRequest.setFunctionUrn(functionUrn);
        invokeFunctionRequest.setBody(objectMapper.readValue(record.getValue(), typeRef));
        if (log.isDebugEnabled()) {
            log.debug("begin to send functiongraph message id is {}", message.getMessageId());
        }
        final CompletableFuture<AsyncInvokeFunctionResponse> future = functionGraphAsyncClient.asyncInvokeFunctionAsync(invokeFunctionRequest);
        future.whenComplete((resp, throwable) -> {
            if (throwable != null) {
                log.error("call function graph {} error is ", message.getMessageId(), throwable);
                record.fail();
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("success to send functiongraph, message id is {}", message.getMessageId());
            }
            record.ack();
        });
    }

    @Override
    public void close() throws Exception {
    }
}
