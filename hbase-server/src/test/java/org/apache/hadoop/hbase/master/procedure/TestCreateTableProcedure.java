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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MasterTests.class, MediumTests.class})
public class TestCreateTableProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestCreateTableProcedure.class);

  private static final String F1 = "f1";
  private static final String F2 = "f2";

  @Rule public TestName name = new TestName();

  @Test(timeout=60000)
  public void testSimpleCreate() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[][] splitKeys = null;
    testSimpleCreate(tableName, splitKeys);
  }

  @Test(timeout=60000)
  public void testSimpleCreateWithSplits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    testSimpleCreate(tableName, splitKeys);
  }

  private void testSimpleCreate(final TableName tableName, byte[][] splitKeys) throws Exception {
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, F1, F2);
    MasterProcedureTestingUtility.validateTableCreation(getMaster(), tableName, regions, F1, F2);
  }

  @Test(timeout=60000)
  public void testCreateWithoutColumnFamily() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // create table with 0 families will fail
    final TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(MasterProcedureTestingUtility.createHTD(tableName));

    // disable sanity check
    builder.setValue("hbase.table.sanity.checks", Boolean.FALSE.toString());
    TableDescriptor htd = builder.build();
    final RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, null);

    long procId =
        ProcedureTestingUtility.submitAndWait(procExec,
            new CreateTableProcedure(procExec.getEnvironment(), htd, regions));
    final Procedure<?> result = procExec.getResult(procId);
    assertEquals(true, result.isFailed());
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue("expected DoNotRetryIOException, got " + cause,
        cause instanceof DoNotRetryIOException);
  }

  @Test(timeout=60000, expected=TableExistsException.class)
  public void testCreateExisting() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f");
    final RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, null);

    // create the table
    long procId1 = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions));

    // create another with the same name
    ProcedurePrepareLatch latch2 = new ProcedurePrepareLatch.CompatibilityLatch();
    long procId2 = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions, latch2));

    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));

    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    latch2.await();
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // create the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Create procedure && kill the executor
    byte[][] splitKeys = null;
    TableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1", "f2");
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, splitKeys);
    long procId = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
    MasterProcedureTestingUtility.validateTableCreation(getMaster(), tableName, regions, F1, F2);
  }

  @Test(timeout=90000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    testRollbackAndDoubleExecution(TableDescriptorBuilder.newBuilder(MasterProcedureTestingUtility.createHTD(tableName, F1, F2)));
  }

  @Test(timeout=90000)
  public void testRollbackAndDoubleExecutionOnMobTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, F1, F2);
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(htd)
            .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(htd.getColumnFamily(Bytes.toBytes(F1)))
                    .setMobEnabled(true)
                    .build());
    testRollbackAndDoubleExecution(builder);
  }

  private void testRollbackAndDoubleExecution(TableDescriptorBuilder builder) throws Exception {
    // create the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Create procedure && kill the executor
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    builder.setRegionReplication(3);
    TableDescriptor htd = builder.build();
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, splitKeys);
    long procId = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions));

    int numberOfSteps = 0; // failing at pre operation
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);

    TableName tableName = htd.getTableName();
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);

    // are we able to create the table after a rollback?
    resetProcExecutorTestingKillFlag();
    testSimpleCreate(tableName, splitKeys);
  }

  @Test
  public void testMRegions() throws Exception {
    final byte[][] splitKeys = new byte[500][];
    for (int i = 0; i < splitKeys.length; ++i) {
      splitKeys[i] = Bytes.toBytes(String.format("%08d", i));
    }

    final TableDescriptor htd = MasterProcedureTestingUtility.createHTD(
      TableName.valueOf("TestMRegions"), F1, F2);
    UTIL.getAdmin().createTableAsync(htd, splitKeys)
      .get(10, java.util.concurrent.TimeUnit.HOURS);
    LOG.info("TABLE CREATED");
  }
}
