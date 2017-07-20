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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JobStatus;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CompleteOperation;
import com.hazelcast.jet.impl.operation.ExecuteOperation;
import com.hazelcast.jet.impl.operation.InitOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.JobStatus.*;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class MasterContext {

    private final NodeEngineImpl nodeEngine;

    private final JetService jetService;

    private final ILogger logger;

    private final long jobId;

    private final DAG dag;

    private final CompletableFuture<Throwable> completionFuture = new CompletableFuture<>();

    private final AtomicReference<JobStatus> jobStatus = new AtomicReference<>(NOT_STARTED);

    private volatile long executionId;

    private volatile long jobStartTime;

    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;

    public MasterContext(NodeEngineImpl nodeEngine, JetService jetService, long jobId, DAG dag) {
        this.nodeEngine = nodeEngine;
        this.jetService = jetService;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobId = jobId;
        this.dag = dag;
    }

    public long getJobId() {
        return jobId;
    }

    public long getExecutionId() {
        return executionId;
    }

    public CompletableFuture<Throwable> getCompletionFuture() {
        return completionFuture;
    }

    public JobStatus getJobStatus() {
        return jobStatus.get();
    }

    public CompletableFuture<Throwable> start() {
        if (checkJobStatusForStart()) {
            return completionFuture;
        }

        executionId = jetService.getJobRepository().generateRandomId();

        logger.info("Start executing job " + jobId + " execution " + executionId + " status " + getJobStatus()
                + " : " + dag);
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        MembersView membersView = clusterService.getMembershipManager().getMembersView();
        logger.fine("Building execution plan for job " + jobId + " execution " + executionId);
        try {
            executionPlanMap = jetService.createExecutionPlans(membersView, dag);
        } catch (TopologyChangedException e) {
            logger.severe("Execution plans could not be created for job: " + jobId
                    + " execution " + executionId, e);
            jetService.scheduleRestart(jobId);
            return completionFuture;
        }

        logger.fine("Built execution plan for job " + jobId + " execution " + executionId + '.');

        Set<MemberInfo> participants = executionPlanMap.keySet();

        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitOperation(jobId, executionId, membersView.getVersion(), participants, plan);
        invoke(operationCtor, this::onInitStepCompleted, null);

        return completionFuture;
    }

    private boolean checkJobStatusForStart() {
        JobStatus status = getJobStatus();
        if (status == COMPLETED || status == FAILED) {
            throw new IllegalStateException("Cannot init job: " + jobId + " since it already is " + status);
        }

        if (completionFuture.isCancelled()) {
            logger.fine("Skipping init because job " + jobId + " because is already cancelled.");
            onCompleteStepCompleted(null);
            return true;
        }

        if (status == NOT_STARTED) {
            if (!jobStatus.compareAndSet(NOT_STARTED, STARTING)) {
                logger.fine("Cannot init job: " + jobId + " someone else is just starting it");
                return true;
            }

            jobStartTime = System.currentTimeMillis();
        } else {
            jobStatus.compareAndSet(RUNNING, RESTARTING);
        }

        status = getJobStatus();
        if (!(status == STARTING || status == RESTARTING)) {
            throw new IllegalStateException("Cannot init job: " + jobId + " since it is " + status);
        }

        return false;
    }

    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getInitResult(responses);

        if (error == null) {
            invokeExecute();
        } else {
            invokeComplete(error);
        }
    }

    private Throwable getInitResult(Map<MemberInfo, Object> responses) {
        if (completionFuture.isCancelled()) {
            logger.fine("job " + jobId + " execution " + executionId + " to be cancelled after init");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Init of job: " + jobId + " execution " + executionId + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Init of job: " + jobId + " execution " + executionId + " failed with: " + failures);

        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !isTopologyChangeFailure(t))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private Map<Boolean, List<Entry<MemberInfo, Object>>> groupResponses(Map<MemberInfo, Object> responses) {
        return responses
                    .entrySet()
                    .stream()
                    .collect(partitioningBy(e -> e.getValue() instanceof Throwable));
    }

    private boolean isTopologyChangeFailure(Object response) {
        return response instanceof MemberLeftException
                || response instanceof TargetNotMemberException
                || response instanceof CallerNotMemberException;
    }

    private void invokeExecute() {
        JobStatus status = getJobStatus();

        if (!(status == STARTING || status == RESTARTING)) {
            throw new IllegalStateException("Cannot execute job " + jobId + " execution " + executionId
                    + " since it is " + status);
        }

        jobStatus.set(RUNNING);
        logger.fine("Executing job: " + jobId + " execution " + executionId);
        Function<ExecutionPlan, Operation> operationCtor = plan -> new ExecuteOperation(jobId, executionId);
        invoke(operationCtor, this::onExecuteStepCompleted, completionFuture);
    }

    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        invokeComplete(getExecuteResult(responses));
    }

    private Throwable getExecuteResult(Map<MemberInfo, Object> responses) {
        if (completionFuture.isCancelled()) {
            logger.fine("job " + jobId + " execution " + executionId + " to be cancelled after execute");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Execute of job: " + jobId + " execution " + executionId + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Execute of job: " + jobId + " execution " + executionId + " has failures: " + failures);

        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException || isTopologyChangeFailure(t)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private void invokeComplete(Throwable error) {
        JobStatus status = getJobStatus();

        if (status == NOT_STARTED || status == COMPLETED || status == FAILED) {
            throw new IllegalStateException("Cannot complete job " + jobId + " execution " + executionId
                    + " since it is " + status);
        }

        logger.fine("Completing job " + jobId + " execution " + executionId);

        Function<ExecutionPlan, Operation> operationCtor = plan -> new CompleteOperation(jobId, executionId, error);
        invoke(operationCtor, responses -> onCompleteStepCompleted(error), null);
    }

    private void onCompleteStepCompleted(Throwable failure) {
        long completionTime = System.currentTimeMillis();

        if (failure instanceof TopologyChangedException) {
            start();
            return;
        }

        if (failure instanceof CancellationException) {
            failure = null;
        }

        long elapsed = completionTime - jobStartTime;

        if (failure == null) {
            jobStatus.set(COMPLETED);
            logger.info("Execution of job " + jobId + " execution " + executionId
                    + " completed in " + elapsed + " ms.");
        } else {
            jobStatus.set(FAILED);
            logger.warning("Execution of job " + jobId + " execution " + executionId
                    + " failed in " + elapsed + " ms.", failure);
        }

        jetService.completeJob(this, completionTime, failure);
        completionFuture.complete(failure);
    }

    private void invoke(Function<ExecutionPlan, Operation> operationCtor,
                        Consumer<Map<MemberInfo, Object>> completionCallback,
                        CompletableFuture cancellation) {
        CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        Map<MemberInfo, InternalCompletableFuture<Object>> futures = new ConcurrentHashMap<>();
        invokeOnParticipants(futures, doneFuture, operationCtor);

        // once all invocations return, notify the completion callback
        doneFuture.whenComplete((aVoid, throwable) -> {
            Map<MemberInfo, Object> responses = new HashMap<>();
            for (Entry<MemberInfo, InternalCompletableFuture<Object>> entry : futures.entrySet()) {
                Object val;
                try {
                    val = entry.getValue().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    val = e;
                } catch (Exception e) {
                    val = peel(e);
                }

                responses.put(entry.getKey(), val);
            }

            completionCallback.accept(responses);
        });

        boolean cancelOnFailure = (cancellation != null);

        // if cancel on failure is true, we should cancel invocations when the given future is cancelled, or
        // any of the invocations fail

        if (cancelOnFailure) {
            cancellation.whenComplete((r, e) -> {
                if (e instanceof CancellationException) {
                    futures.values().forEach(f -> f.cancel(true));
                }
            });

            ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                }

                @Override
                public void onFailure(Throwable t) {
                    futures.values().forEach(f -> f.cancel(true));
                }
            };

            futures.values().forEach(f -> f.andThen(callback));
        }
    }

    private void invokeOnParticipants(Map<MemberInfo, InternalCompletableFuture<Object>> futures,
                                      CompletableFuture<Void> doneFuture,
                                      Function<ExecutionPlan, Operation> opCtor) {
        AtomicInteger doneLatch = new AtomicInteger(executionPlanMap.size());

        for (Entry<MemberInfo, ExecutionPlan> e : executionPlanMap.entrySet()) {
            MemberInfo member = e.getKey();
            Operation op = opCtor.apply(e.getValue());
            InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, op, member.getAddress())
                    .setDoneCallback(() -> {
                        if (doneLatch.decrementAndGet() == 0) {
                            doneFuture.complete(null);
                        }
                    })
                    .invoke();
            futures.put(member, future);
        }
    }

}
