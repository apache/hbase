/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestOldestProcedureMetrics {

  private static final MetricsAssertHelper METRICS_HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @AfterEach
  public void resetEnvironmentEdge() {
    EnvironmentEdgeManager.reset();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOldestProcedureAge() {
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    edge.setValue(10_000L);
    EnvironmentEdgeManager.injectEdge(edge);

    HMaster master = mock(HMaster.class);
    ProcedureExecutor<MasterProcedureEnv> procedureExecutor = mock(ProcedureExecutor.class);
    LockProcedure oldestLock = mock(LockProcedure.class);
    LockProcedure newerLock = mock(LockProcedure.class);
    ServerCrashProcedure serverCrash = mock(ServerCrashProcedure.class);
    ServerCrashProcedure finished = mock(ServerCrashProcedure.class);

    when(master.getMasterProcedureExecutor()).thenReturn(procedureExecutor);
    when(procedureExecutor.getActiveProceduresNoCopy())
      .thenReturn(List.of(oldestLock, newerLock, serverCrash, finished));
    when(oldestLock.getSubmittedTime()).thenReturn(1_000L);
    when(newerLock.getSubmittedTime()).thenReturn(4_000L);
    when(serverCrash.getSubmittedTime()).thenReturn(2_000L);
    when(finished.isFinished()).thenReturn(true);
    when(finished.getSubmittedTime()).thenReturn(100L);

    MetricsMasterWrapperImpl wrapper = new MetricsMasterWrapperImpl(master);
    assertEquals(Map.of("LockProcedure", 9_000L, "ServerCrashProcedure", 8_000L),
      wrapper.getOldestProcedureAgeByType());

    MetricsMasterProcSource source = new MetricsMasterProcSourceImpl(wrapper);
    METRICS_HELPER.assertGauge(MetricsMasterProcSource.OLDEST_PROCEDURE_AGE_NAME, 9_000L, source);
    METRICS_HELPER.assertGauge(MetricsMasterProcSource.OLDEST_PROCEDURE_AGE_NAME + "_LockProcedure",
      9_000L, source);
    METRICS_HELPER.assertGauge(
      MetricsMasterProcSource.OLDEST_PROCEDURE_AGE_NAME + "_ServerCrashProcedure", 8_000L, source);

    when(procedureExecutor.getActiveProceduresNoCopy()).thenReturn(List.of());
    assertEquals(Map.of(), wrapper.getOldestProcedureAgeByType());

    when(procedureExecutor.getActiveProceduresNoCopy()).thenReturn(List.of(newerLock));
    when(newerLock.getSubmittedTime()).thenReturn(11_000L);
    assertEquals(Map.of("LockProcedure", 0L), wrapper.getOldestProcedureAgeByType());

    when(master.getMasterProcedureExecutor()).thenReturn(null);
    assertEquals(Map.of(), wrapper.getOldestProcedureAgeByType());
  }
}
