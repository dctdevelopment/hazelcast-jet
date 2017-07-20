/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public class ExecuteOperation extends AsyncExecutionOperation {

    private volatile CompletionStage<Void> executionFuture;

    private long executionId;

    public ExecuteOperation(long jobId, long executionId) {
        super(jobId);
        this.executionId = executionId;
    }

    private ExecuteOperation() {
        // for deserialization
    }

    @Override
    protected void doRun() throws Exception {
        ILogger logger = getLogger();
        JetService service = getService();
        logger.info("Start execution of job " + jobId + " execution  " + executionId
                + " from caller " + getCallerAddress() + '.');

        executionFuture = service.execute(getCallerAddress(), jobId, executionId, f -> f.handle((r, error) -> error)
                .thenAccept((value) -> {
                    if (value != null) {
                        logger.fine("Execution of job " + jobId + " execution " + executionId
                                + " completed with failure.", value);
                    } else {
                        logger.fine("Execution of job " + jobId + " execution " + executionId + " completed.");
                    }

                    doSendResponse(value);
                }));
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }

        return super.onInvocationException(throwable);
    }

    @Override
    public void cancel() {
        if (executionFuture != null) {
            executionFuture.toCompletableFuture().cancel(true);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
    }
}
