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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.JobResultRepository;
import com.hazelcast.jet.impl.execution.JobResult;
import com.hazelcast.jet.impl.execution.MasterContext;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.jet.TestUtil.getJetService;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.*;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class TopologyChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int PARALLELISM = 4;

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance[] instances;
    private JetTestInstanceFactory factory;

    @Before
    public void setup() {
        MockSupplier.completeCount.set(0);
        MockSupplier.initCount.set(0);
        MockSupplier.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * PARALLELISM);

        factory = new JetTestInstanceFactory();
        JetConfig config = new JetConfig();
//        config.getHazelcastConfig().setProperty("hazelcast.partition.count", "27100");
        config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
        config.getHazelcastConfig().getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
        instances = factory.newMembers(config, NODE_COUNT);
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_addNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Future<Void> future = instances[0].newJob(dag).execute();
        StuckProcessor.executionStarted.await();
        factory.newMember();
        StuckProcessor.proceedLatch.countDown();
        future.get();

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
            assertThat(MockSupplier.completeErrors, empty());
        });
    }

    @Test
    public void when_addAndRemoveNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Future<Void> future = instances[0].newJob(dag).execute();
        StuckProcessor.executionStarted.await();
        JetInstance instance = factory.newMember();
        instance.shutdown();
        StuckProcessor.proceedLatch.countDown();
        future.get();

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
            assertThat(MockSupplier.completeErrors, empty());
        });
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecution_then_jobRestarts() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Future<Void> future = instances[0].newJob(dag).execute();
        StuckProcessor.executionStarted.await();

        instances[1].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        future.get();

        // upon non-coordinator member leave, remaining members restart and complete the job
        final int count = NODE_COUNT * 2 - 1;
        assertEquals(count, MockSupplier.initCount.get());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(count, MockSupplier.completeCount.get());
                assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());
                for (int i = 0; i < MockSupplier.completeErrors.size(); i++) {
                    Throwable error = MockSupplier.completeErrors.get(i);
                    assertTrue(error instanceof TopologyChangedException || error instanceof HazelcastInstanceNotActiveException);
                }
            }
        });
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_jobCompletes() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Long jobId = null;
        try {
            Future<Void> future = instances[0].newJob(dag).execute();
            StuckProcessor.executionStarted.await();

            Map<Long, MasterContext> masterContexts = getJetService(instances[0]).getMasterContexts();
            assertEquals(1, masterContexts.size());
            MasterContext masterContext = masterContexts.values().iterator().next();
            jobId = masterContext.getJobId();

            instances[0].getHazelcastInstance().getLifecycleService().terminate();
            StuckProcessor.proceedLatch.countDown();

            future.get();
            fail();
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof HazelcastInstanceNotActiveException);
        }

        // Then
        assertNotNull(jobId);
        final long completedJobId = jobId;

        JobResultRepository jobResultRepository = getJetService(instances[1]).getJobResultRepository();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                JobResult jobResult = jobResultRepository.getJobResult(completedJobId);
                assertNotNull(jobResult);
                assertTrue(jobResult.isSuccessful());
            }
        });

        final int count = 2 * NODE_COUNT - 1;
        assertEquals(count, MockSupplier.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(count, MockSupplier.completeCount.get());
            assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());
            for (int i = 0; i < MockSupplier.completeErrors.size(); i++) {
                Throwable error = MockSupplier.completeErrors.get(i);
                assertTrue(error instanceof TopologyChangedException || error instanceof HazelcastInstanceNotActiveException);
            }
        });
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_jobCompletes_2() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Future<Void> future = instances[2].newJob(dag).execute();
        StuckProcessor.executionStarted.await();

        Map<Long, MasterContext> masterContexts = getJetService(instances[0]).getMasterContexts();
        assertEquals(1, masterContexts.size());
        MasterContext masterContext = masterContexts.values().iterator().next();
        final long jobId = masterContext.getJobId();

        instances[0].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        future.get();


        // Then
        JobResultRepository jobResultRepository = getJetService(instances[1]).getJobResultRepository();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                JobResult jobResult = jobResultRepository.getJobResult(jobId);
                assertNotNull(jobResult);
                assertTrue(jobResult.isSuccessful());
            }
        });

        final int count = 2 * NODE_COUNT - 1;
        assertEquals(count, MockSupplier.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(count, MockSupplier.completeCount.get());
            assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());
            for (int i = 0; i < MockSupplier.completeErrors.size(); i++) {
                Throwable error = MockSupplier.completeErrors.get(i);
                assertTrue(error instanceof TopologyChangedException || error instanceof HazelcastInstanceNotActiveException);
            }
        });
    }

    static class MockSupplier implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger completeCount = new AtomicInteger();
        static List<Throwable> completeErrors = new CopyOnWriteArrayList<>();

        private final RuntimeException initError;
        private final DistributedSupplier<Processor> supplier;
        private final int nodeCount;

        private boolean initCalled;

        MockSupplier(DistributedSupplier<Processor> supplier, int nodeCount) {
            this(null, supplier, nodeCount);
        }

        MockSupplier(RuntimeException initError, DistributedSupplier<Processor> supplier, int nodeCount) {
            this.initError = initError;
            this.supplier = supplier;
            this.nodeCount = nodeCount;
        }

        @Override
        public void init(@Nonnull Context context) {
            initCalled = true;
            initCount.incrementAndGet();

            if (initError != null) {
                throw initError;
            }
        }

        @Override  @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            if (error != null) {
                completeErrors.add(error);
            }
            completeCount.incrementAndGet();
            if (!initCalled) {
                throw new IllegalStateException("Complete called without calling init()");
            }

            if (completeCount.get() > initCount.get()) {
                throw new IllegalStateException("Complete called " + completeCount.get() + " but init called "
                        + initCount.get() + " times!");
            }

            if (initCount.get() < nodeCount) {
                throw new IllegalStateException("Complete called without init being called on all the nodes! init count: "
                        + initCount.get() + " node count: " + nodeCount);
            }
        }
    }

    static final class StuckProcessor implements Processor {
        static volatile CountDownLatch executionStarted;
        static volatile CountDownLatch proceedLatch;

        @Override
        public boolean complete() {
            executionStarted.countDown();
            try {
                proceedLatch.await();
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
            return true;
        }
    }
}
