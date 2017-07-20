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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.*;
import com.hazelcast.jet.impl.execution.ExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.hazelcast.util.executor.ExecutorType.CACHED;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;

public class JetService
        implements ManagedService, ConfigurableService<JetConfig>, PacketHandler, LiveOperationsTracker,
        CanCancelOperations, MembershipAwareService {

    public static final String SERVICE_NAME = "hz:impl:jetService";

    private static final String COORDINATOR_EXECUTOR_NAME = "jet:coordinator";

    private static final long JOB_SCANNER_TASK_PERIOD_IN_MILLIS = TimeUnit.SECONDS.toMillis(1);

    private final ILogger logger;
    private final ClientInvocationRegistry clientInvocationRegistry;
    private final LiveOperationRegistry liveOperationRegistry;

    private final Lock coordinatorLock = new ReentrantLock();
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();
    private final Set<Long> executionContextJobIds = newSetFromMap(new ConcurrentHashMap<>());
    // key: executionId
    private final ConcurrentMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();
    // The type of these variables is CHM and not ConcurrentMap because we
    // rely on specific semantics of computeIfAbsent. ConcurrentMap.computeIfAbsent
    // does not guarantee at most one computation per key.
    // key: jobId
    private final ConcurrentHashMap<Long, JetClassLoader> classLoaders = new ConcurrentHashMap<>();

    private JetConfig config = new JetConfig();
    private NodeEngineImpl nodeEngine;
    private JetInstance jetInstance;
    private Networking networking;
    private ExecutionService executionService;
    private JobRepository jobRepository;
    private JobResultRepository jobResultRepository;

    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.clientInvocationRegistry = new ClientInvocationRegistry();
        this.liveOperationRegistry = new LiveOperationRegistry();
    }

    @Override
    public void configure(JetConfig config) {
        this.config = config;
    }


    // ManagedService

    @Override
    public void init(NodeEngine engine, Properties properties) {
        jetInstance = new JetInstanceImpl((HazelcastInstanceImpl) engine.getHazelcastInstance(), config);
        networking = new Networking(engine, executionContexts, config.getInstanceConfig().getFlowControlPeriodMs());
        executionService = new ExecutionService(nodeEngine.getHazelcastInstance(),
                config.getInstanceConfig().getCooperativeThreadCount());
        jobRepository = new JobRepository(nodeEngine.getHazelcastInstance());
        jobResultRepository = new JobResultRepository(nodeEngine, jobRepository);

        nodeEngine.getExecutionService().register(COORDINATOR_EXECUTOR_NAME, 2, Integer.MAX_VALUE, CACHED);
        nodeEngine.getExecutionService().scheduleWithRepetition(COORDINATOR_EXECUTOR_NAME, this::scanStartableJobs,
                0, JOB_SCANNER_TASK_PERIOD_IN_MILLIS, MILLISECONDS);

        ClientEngineImpl clientEngine = engine.getService(ClientEngineImpl.SERVICE_NAME);
        ExceptionUtil.registerJetExceptions(clientEngine.getClientExceptionFactory());

        JetBuildInfo jetBuildInfo = BuildInfoProvider.getBuildInfo().getJetBuildInfo();
        logger.info("Starting Jet " + jetBuildInfo.getVersion() + " (" + jetBuildInfo.getBuild() + " - " +
                jetBuildInfo.getRevision() + ") ");
        logger.info("Setting number of cooperative threads and default parallelism to "
                + config.getInstanceConfig().getCooperativeThreadCount());

        logger.info('\n' +
                "\to   o   o   o---o o---o o     o---o   o   o---o o-o-o        o o---o o-o-o\n" +
                "\t|   |  / \\     /  |     |     |      / \\  |       |          | |       |  \n" +
                "\to---o o---o   o   o-o   |     o     o---o o---o   |          | o-o     |  \n" +
                "\t|   | |   |  /    |     |     |     |   |     |   |      \\   | |       |  \n" +
                "\to   o o   o o---o o---o o---o o---o o   o o---o   o       o--o o---o   o   ");
        logger.info("Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.");
    }

    @Override
    public void shutdown(boolean terminate) {
        networking.destroy();
        executionService.shutdown();
        executionContexts.values().forEach(e -> e.cancel().whenComplete((aVoid, throwable) -> {
            long executionId = e.getExecutionId();
            completeExecution(executionId, new HazelcastInstanceNotActiveException());
        }));
    }

    @Override
    public void reset() {
    }

    // End ManagedService


    public void initExecution(long jobId, long executionId, Address coordinator, int coordinatorMemberListVersion,
                              Set<MemberInfo> participants, ExecutionPlan plan) {
        verifyCoordinator(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);

        if (!executionContextJobIds.add(jobId)) {
            ExecutionContext current = executionContexts.get(executionId);
            if (current != null) {
                throw new IllegalStateException("Execution context for job " + jobId + " execution " + executionId
                        + " for coordinator " + coordinator + " already exists for coordinator "
                        + current.getCoordinator());
            }

            executionContexts.values().stream()
                    .filter(e -> e.getJobId() == jobId)
                    .forEach(e -> logger.fine("Execution context for job " + jobId + " execution " + executionId
                            + " for coordinator " + coordinator + " already exists with local execution " + e.getJobId()
                            + " for coordinator " + e.getCoordinator()));

            throw new RetryableHazelcastException();
        }

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ExecutionContext created = new ExecutionContext(nodeEngine, executionService,
                jobId, executionId, coordinator, addresses);
        try {
            created.initialize(plan);
        } finally {
            executionContexts.put(executionId, created);
        }
    }

    private void verifyCoordinator(long jobId, long executionId, Address coordinator,
                                   int coordinatorMemberListVersion, Set<MemberInfo> participants) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!masterAddress.equals(coordinator)) {
            throw new IllegalStateException("Coordinator: " + coordinator + " cannot init job " + jobId
                    + " execution " + executionId + " because it is not master: " + masterAddress);
        }

        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();
        int localMemberListVersion = membershipManager.getMemberListVersion();
        if (coordinatorMemberListVersion > localMemberListVersion) {
            nodeEngine.getOperationService().send(new TriggerMemberListPublishOp(), masterAddress);
            throw new RetryableHazelcastException("Cannot initialize job " + jobId + " execution " + executionId
                    + " for coordinator: " + coordinator + " Local member list version: " + localMemberListVersion
                    + " coordinator member list version: " + coordinatorMemberListVersion);
        }

        for (MemberInfo participant : participants) {
            if (membershipManager.getMember(participant.getAddress(), participant.getUuid()) == null) {
                throw new IllegalStateException("Cannot initialize job " + jobId + " execution " + executionId
                        + " for coordinator: " + coordinator + " since participant: " + participant
                        + " not found in local member list. Local member list version: " + localMemberListVersion
                        + " coordinator member list version: " + coordinatorMemberListVersion);
            }
        }
    }

    public CompletionStage<Void> execute(Address coordinator, long jobId, long executionId,
                                         Consumer<CompletionStage<Void>> doneCallback) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!masterAddress.equals(coordinator)) {
            throw new IllegalStateException("Coordinator: " + coordinator + " cannot start job " + jobId
                   + " execution " + executionId + " because it is not master: " + masterAddress);
        }

        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext == null) {
            throw new IllegalStateException("Job " + jobId + " execution " + executionId
                    + " not found for coordinator: " + coordinator + " for execution start");
        } else if (!executionContext.verify(coordinator, jobId)) {
            throw new IllegalStateException("Job " + jobId +  " execution " + executionContext.getExecutionId()
                    + " of coordinator: " + executionContext.getCoordinator() + " cannot be started by: "
                    + coordinator + " and execution " + executionId);
        }

        return executionContext.execute(doneCallback);
    }

    public void completeExecution(long executionId, Throwable error) {
        ExecutionContext executionContext = executionContexts.remove(executionId);
        if (executionContext != null) {
            executionContext.complete(error);
            classLoaders.remove(executionContext.getJobId());
            executionContextJobIds.remove(executionContext.getJobId());
        } else {
            logger.fine("Execution " + executionId + " not found for completion");
        }
    }

    public JetInstance getJetInstance() {
        return jetInstance;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    public ClientInvocationRegistry getClientInvocationRegistry() {
        return clientInvocationRegistry;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public JobResultRepository getJobResultRepository() {
        return jobResultRepository;
    }

    public ClassLoader getClassLoader(long jobId) {
        return classLoaders.computeIfAbsent(jobId, k -> AccessController.doPrivileged(
                (PrivilegedAction<JetClassLoader>) () -> new JetClassLoader(jobRepository, jobId)
        ));
    }

    public Set<Long> getExecutionIds() {
        return new HashSet<>(executionContexts.keySet());
    }

    public ExecutionContext getExecutionContext(long executionId) {
        return executionContexts.get(executionId);
    }

    public Map<MemberInfo, ExecutionPlan> createExecutionPlans(MembersView membersView, DAG dag) {
        return ExecutionPlanBuilder.createExecutionPlans(nodeEngine, membersView, dag,
                config.getInstanceConfig().getCooperativeThreadCount());
    }


    // LiveOperationsTracker

    @Override
    public void populate(LiveOperations liveOperations) {
        liveOperationRegistry.populate(liveOperations);
    }

    @Override
    public boolean cancelOperation(Address caller, long callId) {
        return liveOperationRegistry.cancel(caller, callId);
    }


    // PacketHandler

    @Override
    public void handle(Packet packet) throws IOException {
        networking.handle(packet);
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        Address address = event.getMember().getAddress();

        // complete the processors, whose caller is dead, with TopologyChangedException
        executionContexts.values()
                .stream()
                .filter(e -> e.isCoordinatorOrParticipating(address))
                .forEach(e -> e.cancel().whenComplete((aVoid, throwable) -> {
                    long executionId = e.getExecutionId();
                    logger.fine("Completing job " + e.getJobId() + " execution: " + executionId
                            + " locally because " + address + " left...");
                    completeExecution(executionId, new TopologyChangedException("Topology has been changed"));
                }));
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public Map<Long, MasterContext> getMasterContexts() {
        return new HashMap<>(masterContexts);
    }

    public CompletableFuture<Throwable> startOrJoinJob(long jobId) {
        if (!nodeEngine.getClusterService().isMaster()) {
            throw new JetException("Job cannot be started here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        MasterContext newMasterContext;
        coordinatorLock.lock();
        try {
            JobResult jobResult = jobResultRepository.getJobResult(jobId);
            if (jobResult != null) {
                logger.fine("Not starting job " + jobId + " since already completed -> " + jobResult);
                return jobResult.asCompletableFuture();
            }

            StartableJob startableJob = jobRepository.getStartableJob(jobId);
            if (startableJob == null) {
                throw new IllegalStateException("Job " + jobId + " not found");
            }

            MasterContext currentMasterContext = masterContexts.get(jobId);
            if (currentMasterContext != null) {
                return currentMasterContext.getCompletionFuture();
            }

            newMasterContext = new MasterContext(nodeEngine, this, jobId, startableJob.getDag());
            masterContexts.put(jobId, newMasterContext);

            logger.info("Starting new job " + jobId);

        } finally {
            coordinatorLock.unlock();
        }

        return newMasterContext.start();
    }

    public void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            logger.fine("Scheduling master context restart for " + jobId);
            nodeEngine.getExecutionService()
                    .schedule(COORDINATOR_EXECUTOR_NAME, masterContext::start, 250, MILLISECONDS);
        } else {
            logger.info("Master context for job " + jobId + " not found to schedule restart" );
        }
    }

    public void completeJob(MasterContext masterContext, long completionTime, Throwable error) {
        coordinatorLock.lock();
        long jobId = masterContext.getJobId(), executionId = masterContext.getExecutionId();
        try {
            if (masterContexts.remove(masterContext.getJobId(), masterContext)) {
                long jobCreationTime = jobRepository.getJobCreationTimeOrFail(jobId);
                Address coordinator = nodeEngine.getThisAddress();
                JobResult jobResult = new JobResult(jobId, coordinator, jobCreationTime, completionTime, error);
                jobResultRepository.completeJob(jobResult);

                logger.fine("Job " + jobId + " execution " + executionId + " is completed.");
            } else {
                MasterContext existing = masterContexts.get(jobId);
                if (existing != null) {
                    logger.severe("Different master context found to complete job " + jobId
                            + " execution " + executionId + " master context execution " + existing.getExecutionId());
                } else {
                    logger.severe("No master context found to complete job " + jobId + " execution " + executionId);
                }
            }
        } catch (Exception e) {
            logger.severe("Completion of job " + jobId + " execution " + executionId + " is failed.", e);
        } finally {
            coordinatorLock.unlock();
        }
    }

    private void scanStartableJobs() {
        if (!shouldScanStartableJobs()) {
            return;
        }

        Set<Long> jobIds = jobRepository.getAllStartableJobIds();
        if (jobIds.isEmpty()) {
            return;
        }

        try {
            jobIds.forEach(this::startOrJoinJob);
        } catch (Exception e) {
            logger.severe("Scanning startable jobs is failed", e);
        }
    }

    private boolean shouldScanStartableJobs() {
        Node node = nodeEngine.getNode();
        if (!(node.isMaster() && node.isRunning())) {
            return false;
        }

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.isMigrationAllowed() && !partitionService.hasOnGoingMigrationLocal();
    }

}
