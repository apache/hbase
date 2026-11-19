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
package org.apache.hadoop.hbase.master.assignment;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for HBASE-29806: RegionRemoteProcedureBase.afterReplay() causes NPE when parent procedure
 * has already completed.
 * <p>
 * This test verifies that afterReplay() handles the case where the parent
 * TransitRegionStateProcedure has already completed and been moved to the 'completed' map, while
 * the child RegionRemoteProcedureBase is still in the 'procedures' map during restart.
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestRegionRemoteProcedureBaseOrphanAfterReplay {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionRemoteProcedureBaseOrphanAfterReplay.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRegionRemoteProcedureBaseOrphanAfterReplay.class);

  /**
   * Test that afterReplay() does not throw NPE when the parent procedure is not found.
   * <p>
   * This simulates the scenario where:
   * <ol>
   * <li>Parent TransitRegionStateProcedure has completed and been moved to 'completed' map</li>
   * <li>Child RegionRemoteProcedureBase is still pending in 'procedures' map</li>
   * <li>During restart, afterReplay() is called on the child</li>
   * <li>getProcedure(parentProcId) returns null because it only checks 'procedures' map</li>
   * </ol>
   * Before the fix, this would cause a NullPointerException. After the fix, it should log a warning
   * and return gracefully.
   */
  @Test
  public void testAfterReplayWithOrphanedChildProcedure() {
    // Create mock environment
    MasterProcedureEnv env = mock(MasterProcedureEnv.class);
    MasterServices masterServices = mock(MasterServices.class);
    @SuppressWarnings("unchecked")
    ProcedureExecutor<MasterProcedureEnv> procExecutor = mock(ProcedureExecutor.class);

    when(env.getMasterServices()).thenReturn(masterServices);
    when(masterServices.getMasterProcedureExecutor()).thenReturn(procExecutor);

    // When getProcedure is called with any procId, return null
    // This simulates the parent being in the 'completed' map instead of 'procedures' map
    when(procExecutor.getProcedure(1L)).thenReturn(null);

    // Create an OpenRegionProcedure (a concrete subclass of RegionRemoteProcedureBase)
    // Use the no-arg constructor and set fields via reflection or by directly testing
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf("test-table")).build();
    ServerName serverName = ServerName.valueOf("localhost", 12345, 1);

    // Create a test subclass that allows us to set the parent proc id
    TestableOpenRegionProcedure proc = new TestableOpenRegionProcedure(regionInfo, serverName);
    proc.setParentProcId(1L);

    // Call afterReplay - this should NOT throw NPE
    // Before the fix, this would throw: java.lang.NullPointerException
    LOG.info("Calling afterReplay on orphaned child procedure...");
    proc.afterReplay(env);
    LOG.info("afterReplay completed successfully without NPE");

    // If we reach here without exception, the test passes
  }

  /**
   * A testable subclass of OpenRegionProcedure that allows us to set the parent proc id for testing
   * purposes.
   */
  private static class TestableOpenRegionProcedure extends OpenRegionProcedure {
    private long parentProcId;

    public TestableOpenRegionProcedure(RegionInfo region, ServerName targetServer) {
      super();
      this.region = region;
      this.targetServer = targetServer;
    }

    public void setParentProcId(long parentProcId) {
      this.parentProcId = parentProcId;
    }

    @Override
    public long getParentProcId() {
      return this.parentProcId;
    }

    @Override
    public boolean hasParent() {
      return true;
    }
  }
}
