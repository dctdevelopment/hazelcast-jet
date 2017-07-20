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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public class CompleteOperation extends Operation {

    private long jobId;
    private long executionId;
    private Throwable error;

    public CompleteOperation(long jobId, long executionId, Throwable error) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.error = error;
    }

    private CompleteOperation() {
        // for deserialization
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        JetService service = getService();

        Address callerAddress = getCallerAddress();
        logger.fine("Completing execution of plan for job " + jobId + " execution " + executionId
                + " from caller: " + callerAddress + " with " + error);

        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!masterAddress.equals(callerAddress)) {
            throw new IllegalStateException("Caller: " + callerAddress + " cannot complete job " + jobId
                    + " execution " + executionId + " because it is not master: " + masterAddress);
        }

        service.completeExecution(executionId, error);
        logger.fine("Completed execution of plan for job " + jobId + " execution " + + executionId + ".");
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }

        return super.onInvocationException(throwable);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(jobId);
        out.writeLong(executionId);
        out.writeObject(error);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
        executionId = in.readLong();
        error = in.readObject();
    }
}
