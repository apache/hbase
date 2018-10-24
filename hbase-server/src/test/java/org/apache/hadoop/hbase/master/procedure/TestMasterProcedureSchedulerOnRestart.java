/**
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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.DummyRegionProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;


@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureSchedulerOnRestart {
  @ClassRule public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestMasterProcedureSchedulerOnRestart.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestMasterProcedureSchedulerOnRestart.class);
  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final TableName tablename = TableName.valueOf("test:TestProcedureScheduler");
  private static RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tablename).build();

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();


  private static WALProcedureStore procStore;

  private static ProcedureExecutor<MasterProcedureEnv> procExecutor;

  private static HBaseCommonTestingUtility htu;

  private static MasterProcedureEnv masterProcedureEnv;


  private static FileSystem fs;
  private static Path testDir;
  private static Path logDir;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    procExecutor = UTIL.getMiniHBaseCluster().getMaster()
        .getMasterProcedureExecutor();
  }

  @Test
  public void testScheduler() throws Exception {
    // Add a region procedure, but stuck there
    long regionProc = procExecutor.submitProcedure(new DummyRegionProcedure(
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
            .getEnvironment(), regionInfo));
    WALProcedureStore walProcedureStore = (WALProcedureStore) procExecutor.getStore();
    // Roll the wal
    walProcedureStore.rollWriterForTesting();
    Thread.sleep(500);
    // Submit a table procedure
    procExecutor.submitProcedure(new DummyTableProcedure(
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
            .getEnvironment(), tablename));
    // Restart the procExecutor
    ProcedureTestingUtility.restart(procExecutor);
    while (procExecutor.getProcedure(regionProc) == null) {
      Thread.sleep(500);
    }
    DummyRegionProcedure dummyRegionProcedure = (DummyRegionProcedure) procExecutor
        .getProcedure(regionProc);
    // Resume the region procedure
    dummyRegionProcedure.resume();
    // The region procedure should finish normally
    UTIL.waitFor(5000, () -> dummyRegionProcedure.isFinished());

  }

  public static class DummyTableProcedure extends
      AbstractStateMachineTableProcedure<DummyRegionTableState> {

    private TableName tableName;

    public DummyTableProcedure() {
      super();
    }
    public DummyTableProcedure(final MasterProcedureEnv env, TableName tableName) {
      super(null, null);
      this.tableName = tableName;
    }

    @Override
    public TableName getTableName() {
      return tableName;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.CREATE;
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env,
        DummyRegionTableState dummyRegionTableState)
        throws ProcedureSuspendedException, ProcedureYieldException,
        InterruptedException {
      return null;
    }

    @Override
    protected void rollbackState(MasterProcedureEnv env,
        DummyRegionTableState dummyRegionTableState)
        throws IOException, InterruptedException {

    }

    @Override
    protected DummyRegionTableState getState(int stateId) {
      return DummyRegionTableState.STATE;
    }

    @Override
    protected int getStateId(DummyRegionTableState dummyRegionTableState) {
      return 0;
    }

    @Override
    protected DummyRegionTableState getInitialState() {
      return DummyRegionTableState.STATE;
    }

    @Override
    protected Procedure[] execute(final MasterProcedureEnv env)
        throws ProcedureSuspendedException {
      LOG.info("Finished execute");
      return null;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      super.serializeStateData(serializer);
      serializer.serialize(ProtobufUtil.toProtoTableName(tableName));


    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      super.deserializeStateData(serializer);
      tableName = ProtobufUtil
          .toTableName(serializer.deserialize(HBaseProtos.TableName.class));

    }

    @Override
    protected LockState acquireLock(MasterProcedureEnv env) {
      return super.acquireLock(env);
    }

    @Override
    protected void releaseLock(MasterProcedureEnv env) {
      super.releaseLock(env);
    }
  }

  public enum DummyRegionTableState {
    STATE
  }


}
