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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-28876
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestTableProcedureWaitingQueueCleanup {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableProcedureWaitingQueueCleanup.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableDescriptor TD = TableDescriptorBuilder.newBuilder(TableName.valueOf("test"))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

  // In current HBase code base, we do not use table procedure as sub procedure, so here we need to
  // introduce one for testing
  public static class NonTableProcedure extends Procedure<MasterProcedureEnv>
    implements PeerProcedureInterface {

    private boolean created = false;

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (created) {
        return null;
      }
      created = true;
      return new Procedure[] { new CreateTableProcedure(env, TD,
        new RegionInfo[] { RegionInfoBuilder.newBuilder(TD.getTableName()).build() }) };
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    public String getPeerId() {
      return "peer";
    }

    @Override
    public PeerOperationType getPeerOperationType() {
      return PeerOperationType.ENABLE;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  // the root procedure will lock meta but we will schedule a table procedure for other table
  public static class MetaTableProcedure extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface {

    private boolean created = false;

    @Override
    public TableName getTableName() {
      return MetaTableName.getInstance();
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.EDIT;
    }

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (created) {
        return null;
      }
      created = true;
      return new Procedure[] { new CreateTableProcedure(env, TD,
        new RegionInfo[] { RegionInfoBuilder.newBuilder(TD.getTableName()).build() }) };
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  private void testCreateDelete(Procedure<MasterProcedureEnv> proc) throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    assertTrue(UTIL.getAdmin().tableExists(TD.getTableName()));

    // Without the fix in HBASE-28876, we will hang there forever, as we do not clean up the
    // TableProcedureWaitingQueue
    UTIL.getAdmin().disableTable(TD.getTableName());
    UTIL.getAdmin().deleteTable(TD.getTableName());
  }

  @Test
  public void testNonTableProcedure() throws Exception {
    testCreateDelete(new NonTableProcedure());
  }

  @Test
  public void testNotSameTable() throws Exception {
    testCreateDelete(new MetaTableProcedure());
  }
}
