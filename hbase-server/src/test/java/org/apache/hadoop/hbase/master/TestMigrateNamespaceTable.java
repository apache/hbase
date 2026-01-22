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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Testcase for HBASE-21154.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestMigrateNamespaceTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrateNamespaceTable.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static volatile boolean CONTINUE = false;

  // used to halt the migration procedure
  public static final class SuspendProcedure extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface {

    @Override
    public TableName getTableName() {
      try {
        return UTIL.getConnection().getMetaTableName();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.CREATE;
    }

    @Override
    protected LockState acquireLock(final MasterProcedureEnv env) {
      if (env.getProcedureScheduler().waitTableExclusiveLock(this, getTableName())) {
        return LockState.LOCK_EVENT_WAIT;
      }
      return LockState.LOCK_ACQUIRED;
    }

    @Override
    protected void releaseLock(final MasterProcedureEnv env) {
      env.getProcedureScheduler().wakeTableExclusiveLock(this, getTableName());
    }

    @Override
    protected boolean holdLock(MasterProcedureEnv env) {
      return true;
    }

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (CONTINUE) {
        return null;
      }
      throw suspend(1000, true);
    }

    @Override
    protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
      setState(ProcedureProtos.ProcedureState.RUNNABLE);
      env.getProcedureScheduler().addFront(this);
      return false;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return true;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    StartTestingClusterOption option = StartTestingClusterOption.builder().numMasters(1)
      .numAlwaysStandByMasters(1).numRegionServers(1).build();
    UTIL.startMiniCluster(option);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  // simulate upgrading scenario, where we do not have the ns family
  private void removeNamespaceFamily() throws IOException {
    FileSystem fs = UTIL.getTestFileSystem();
    Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, UTIL.getConnection().getMetaTableName());
    TableDescriptor metaTableDesc = FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir);
    TableDescriptor noNsMetaTableDesc = TableDescriptorBuilder.newBuilder(metaTableDesc)
      .removeColumnFamily(HConstants.NAMESPACE_FAMILY).build();
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, noNsMetaTableDesc,
      true);
    for (FileStatus status : fs.listStatus(tableDir)) {
      if (!status.isDirectory()) {
        continue;
      }
      Path familyPath = new Path(status.getPath(), HConstants.NAMESPACE_FAMILY_STR);
      fs.delete(familyPath, true);
    }
  }

  private void addNamespace(Table table, NamespaceDescriptor nd) throws IOException {
    table.put(new Put(Bytes.toBytes(nd.getName())).addColumn(
      TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES,
      TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES,
      ProtobufUtil.toProtoNamespaceDescriptor(nd).toByteArray()));
  }

  @Test
  public void testMigrate() throws IOException, InterruptedException {
    UTIL.getAdmin().createTable(TableDescriptorBuilder.NAMESPACE_TABLEDESC);
    try (Table table = UTIL.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME)) {
      for (int i = 0; i < 5; i++) {
        NamespaceDescriptor nd = NamespaceDescriptor.create("Test-NS-" + i)
          .addConfiguration("key-" + i, "value-" + i).build();
        addNamespace(table, nd);
        AbstractStateMachineNamespaceProcedure
          .createDirectory(UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem(), nd);
      }
      // add default and system
      addNamespace(table, NamespaceDescriptor.DEFAULT_NAMESPACE);
      addNamespace(table, NamespaceDescriptor.SYSTEM_NAMESPACE);
    }
    MasterThread masterThread = UTIL.getMiniHBaseCluster().getMasterThread();
    masterThread.getMaster().getMasterProcedureExecutor().submitProcedure(new SuspendProcedure());
    masterThread.getMaster().stop("For testing");
    masterThread.join();

    removeNamespaceFamily();

    UTIL.getMiniHBaseCluster().startMaster();

    // 5 + default and system('hbase')
    assertEquals(7, UTIL.getAdmin().listNamespaceDescriptors().length);
    for (int i = 0; i < 5; i++) {
      NamespaceDescriptor nd = UTIL.getAdmin().getNamespaceDescriptor("Test-NS-" + i);
      assertEquals("Test-NS-" + i, nd.getName());
      assertEquals(1, nd.getConfiguration().size());
      assertEquals("value-" + i, nd.getConfigurationValue("key-" + i));
    }
    // before migration done, modification on the namespace is not supported
    TableNamespaceManager tableNsMgr =
      UTIL.getMiniHBaseCluster().getMaster().getClusterSchema().getTableNamespaceManager();
    assertThrows(IOException.class, () -> tableNsMgr.deleteNamespace("Test-NS-0"));
    assertThrows(IOException.class,
      () -> tableNsMgr.addOrUpdateNamespace(NamespaceDescriptor.create("NNN").build()));
    CONTINUE = true;
    UTIL.waitFor(30000, () -> UTIL.getAdmin().isTableDisabled(TableName.NAMESPACE_TABLE_NAME));
    // this time it is allowed to change the namespace
    UTIL.getAdmin().createNamespace(NamespaceDescriptor.create("NNN").build());

    masterThread = UTIL.getMiniHBaseCluster().getMasterThread();
    masterThread.getMaster().stop("For testing");
    masterThread.join();
    UTIL.getMiniHBaseCluster().startMaster();

    // make sure that we could still restart the cluster after disabling the namespace table.
    assertEquals(8, UTIL.getAdmin().listNamespaceDescriptors().length);

    // let's delete the namespace table
    UTIL.getAdmin().deleteTable(TableName.NAMESPACE_TABLE_NAME);
    assertFalse(UTIL.getAdmin().tableExists(TableName.NAMESPACE_TABLE_NAME));
  }
}
