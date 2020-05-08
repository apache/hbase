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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MasterProcedureTestingUtility {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcedureTestingUtility.class);

  private MasterProcedureTestingUtility() { }

  public static void restartMasterProcedureExecutor(ProcedureExecutor<MasterProcedureEnv> procExec)
      throws Exception {
    final MasterProcedureEnv env = procExec.getEnvironment();
    final HMaster master = (HMaster) env.getMasterServices();
    ProcedureTestingUtility.restart(procExec, true, true,
      // stop services
      new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          AssignmentManager am = env.getAssignmentManager();
          // try to simulate a master restart by removing the ServerManager states about seqIDs
          for (RegionState regionState: am.getRegionStates().getRegionStates()) {
            env.getMasterServices().getServerManager().removeRegion(regionState.getRegion());
          }
          am.stop();
          master.setInitialized(false);
          return null;
        }
      },
      // setup RIT before starting workers
      new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          AssignmentManager am = env.getAssignmentManager();
          am.start();
          // just follow the same way with HMaster.finishActiveMasterInitialization. See the
          // comments there
          am.setupRIT(procExec.getActiveProceduresNoCopy().stream().filter(p -> !p.isSuccess())
            .filter(p -> p instanceof TransitRegionStateProcedure)
            .map(p -> (TransitRegionStateProcedure) p).collect(Collectors.toList()));
          return null;
        }
      },
      // restart services
      new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          AssignmentManager am = env.getAssignmentManager();
          try {
            am.joinCluster();
            am.wakeMetaLoadedEvent();
            master.setInitialized(true);
          } catch (Exception e) {
            LOG.warn("Failed to load meta", e);
          }
          return null;
        }
      });
  }

  // ==========================================================================
  //  Master failover utils
  // ==========================================================================
  public static void masterFailover(final HBaseTestingUtility testUtil)
      throws Exception {
    MiniHBaseCluster cluster = testUtil.getMiniHBaseCluster();

    // Kill the master
    HMaster oldMaster = cluster.getMaster();
    cluster.killMaster(cluster.getMaster().getServerName());

    // Wait the secondary
    waitBackupMaster(testUtil, oldMaster);
  }

  public static void waitBackupMaster(final HBaseTestingUtility testUtil,
      final HMaster oldMaster) throws Exception {
    MiniHBaseCluster cluster = testUtil.getMiniHBaseCluster();

    HMaster newMaster = cluster.getMaster();
    while (newMaster == null || newMaster == oldMaster) {
      Thread.sleep(250);
      newMaster = cluster.getMaster();
    }

    while (!(newMaster.isActiveMaster() && newMaster.isInitialized())) {
      Thread.sleep(250);
    }
  }

  // ==========================================================================
  //  Table Helpers
  // ==========================================================================
  public static TableDescriptor createHTD(final TableName tableName, final String... family) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (int i = 0; i < family.length; ++i) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family[i]));
    }
    return builder.build();
  }

  public static RegionInfo[] createTable(final ProcedureExecutor<MasterProcedureEnv> procExec,
      final TableName tableName, final byte[][] splitKeys, String... family) throws IOException {
    TableDescriptor htd = createHTD(tableName, family);
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, splitKeys);
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));
    return regions;
  }

  public static void validateTableCreation(final HMaster master, final TableName tableName,
      final RegionInfo[] regions, String... family) throws IOException {
    validateTableCreation(master, tableName, regions, true, family);
  }

  public static void validateTableCreation(final HMaster master, final TableName tableName,
      final RegionInfo[] regions, boolean hasFamilyDirs, String... family) throws IOException {
    // check filesystem
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir =
      CommonFSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    assertTrue(fs.exists(tableDir));
    CommonFSUtils.logFileSystemState(fs, tableDir, LOG);
    List<Path> unwantedRegionDirs = FSUtils.getRegionDirs(fs, tableDir);
    for (int i = 0; i < regions.length; ++i) {
      Path regionDir = new Path(tableDir, regions[i].getEncodedName());
      assertTrue(regions[i] + " region dir does not exist", fs.exists(regionDir));
      assertTrue(unwantedRegionDirs.remove(regionDir));
      List<Path> allFamilyDirs = FSUtils.getFamilyDirs(fs, regionDir);
      for (int j = 0; j < family.length; ++j) {
        final Path familyDir = new Path(regionDir, family[j]);
        if (hasFamilyDirs) {
          assertTrue(family[j] + " family dir does not exist", fs.exists(familyDir));
          assertTrue(allFamilyDirs.remove(familyDir));
        } else {
          // TODO: WARN: Modify Table/Families does not create a family dir
          if (!fs.exists(familyDir)) {
            LOG.warn(family[j] + " family dir does not exist");
          }
          allFamilyDirs.remove(familyDir);
        }
      }
      assertTrue("found extraneous families: " + allFamilyDirs, allFamilyDirs.isEmpty());
    }
    assertTrue("found extraneous regions: " + unwantedRegionDirs, unwantedRegionDirs.isEmpty());
    LOG.debug("Table directory layout is as expected.");

    // check meta
    assertTrue(tableExists(master.getConnection(), tableName));
    assertEquals(regions.length, countMetaRegions(master, tableName));

    // check htd
    TableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue("table descriptor not found", htd != null);
    for (int i = 0; i < family.length; ++i) {
      assertTrue("family not found " + family[i], htd.getColumnFamily(Bytes.toBytes(family[i])) != null);
    }
    assertEquals(family.length, htd.getColumnFamilyCount());
  }

  public static void validateTableDeletion(
      final HMaster master, final TableName tableName) throws IOException {
    // check filesystem
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir =
      CommonFSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    assertFalse(fs.exists(tableDir));

    // check meta
    assertFalse(tableExists(master.getConnection(), tableName));
    assertEquals(0, countMetaRegions(master, tableName));

    // check htd
    assertTrue("found htd of deleted table",
      master.getTableDescriptors().get(tableName) == null);
  }

  private static int countMetaRegions(final HMaster master, final TableName tableName)
      throws IOException {
    final AtomicInteger actualRegCount = new AtomicInteger(0);
    final MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result rowResult) throws IOException {
        RegionLocations list = MetaTableAccessor.getRegionLocations(rowResult);
        if (list == null) {
          LOG.warn("No serialized RegionInfo in " + rowResult);
          return true;
        }
        HRegionLocation l = list.getRegionLocation();
        if (l == null) {
          return true;
        }
        if (!l.getRegionInfo().getTable().equals(tableName)) {
          return false;
        }
        if (l.getRegionInfo().isOffline() || l.getRegionInfo().isSplit()) return true;
        HRegionLocation[] locations = list.getRegionLocations();
        for (HRegionLocation location : locations) {
          if (location == null) continue;
          ServerName serverName = location.getServerName();
          // Make sure that regions are assigned to server
          if (serverName != null && serverName.getAddress() != null) {
            actualRegCount.incrementAndGet();
          }
        }
        return true;
      }
    };
    MetaTableAccessor.scanMetaForTableRegions(master.getConnection(), visitor, tableName);
    return actualRegCount.get();
  }

  public static void validateTableIsEnabled(final HMaster master, final TableName tableName)
      throws IOException {
    TableStateManager tsm = master.getTableStateManager();
    assertTrue(tsm.getTableState(tableName).getState().equals(TableState.State.ENABLED));
  }

  public static void validateTableIsDisabled(final HMaster master, final TableName tableName)
      throws IOException {
    TableStateManager tsm = master.getTableStateManager();
    assertTrue(tsm.getTableState(tableName).getState().equals(TableState.State.DISABLED));
  }

  public static void validateColumnFamilyAddition(final HMaster master, final TableName tableName,
      final String family) throws IOException {
    TableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue(htd != null);

    assertTrue(htd.hasColumnFamily(family.getBytes()));
  }

  public static void validateColumnFamilyDeletion(final HMaster master, final TableName tableName,
      final String family) throws IOException {
    // verify htd
    TableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue(htd != null);
    assertFalse(htd.hasColumnFamily(family.getBytes()));

    // verify fs
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir =
      CommonFSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    for (Path regionDir : FSUtils.getRegionDirs(fs, tableDir)) {
      final Path familyDir = new Path(regionDir, family);
      assertFalse(family + " family dir should not exist", fs.exists(familyDir));
    }
  }

  public static void validateColumnFamilyModification(final HMaster master,
      final TableName tableName, final String family, ColumnFamilyDescriptor columnDescriptor)
      throws IOException {
    TableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue(htd != null);

    ColumnFamilyDescriptor hcfd = htd.getColumnFamily(family.getBytes());
    assertEquals(0, ColumnFamilyDescriptor.COMPARATOR.compare(hcfd, columnDescriptor));
  }

  public static void loadData(final Connection connection, final TableName tableName,
      int rows, final byte[][] splitKeys,  final String... sfamilies) throws IOException {
    byte[][] families = new byte[sfamilies.length][];
    for (int i = 0; i < families.length; ++i) {
      families[i] = Bytes.toBytes(sfamilies[i]);
    }

    BufferedMutator mutator = connection.getBufferedMutator(tableName);

    // Ensure one row per region
    assertTrue(rows >= splitKeys.length);
    for (byte[] k: splitKeys) {
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), k);
      byte[] key = Bytes.add(k, Bytes.toBytes(MD5Hash.getMD5AsHex(value)));
      mutator.mutate(createPut(families, key, value));
      rows--;
    }

    // Add other extra rows. more rows, more files
    while (rows-- > 0) {
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(rows));
      byte[] key = Bytes.toBytes(MD5Hash.getMD5AsHex(value));
      mutator.mutate(createPut(families, key, value));
    }
    mutator.flush();
  }

  private static Put createPut(final byte[][] families, final byte[] key, final byte[] value) {
    byte[] q = Bytes.toBytes("q");
    Put put = new Put(key);
    put.setDurability(Durability.SKIP_WAL);
    for (byte[] family: families) {
      put.addColumn(family, q, value);
    }
    return put;
  }

  // ==========================================================================
  //  Procedure Helpers
  // ==========================================================================
  public static long generateNonceGroup(final HMaster master) {
    return master.getClusterConnection().getNonceGenerator().getNonceGroup();
  }

  public static long generateNonce(final HMaster master) {
    return master.getClusterConnection().getNonceGenerator().newNonce();
  }

  /**
   * Run through all procedure flow states TWICE while also restarting procedure executor at each
   * step; i.e force a reread of procedure store.
   *
   *<p>It does
   * <ol><li>Execute step N - kill the executor before store update
   * <li>Restart executor/store
   * <li>Execute step N - and then save to store
   * </ol>
   *
   *<p>This is a good test for finding state that needs persisting and steps that are not
   * idempotent. Use this version of the test when a procedure executes all flow steps from start to
   * finish.
   * @see #testRecoveryAndDoubleExecution(ProcedureExecutor, long)
   */
  public static void testRecoveryAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep, final boolean expectExecRunning) throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertEquals(false, procExec.isRunning());

    // Restart the executor and execute the step twice
    //   execute step N - kill before store update
    //   restart executor/store
    //   execute step N - save on store
    // NOTE: currently we make assumption that states/ steps are sequential. There are already
    // instances of a procedures which skip (don't use) intermediate states/ steps. In future,
    // intermediate states/ steps can be added with ordinal greater than lastStep. If and when
    // that happens the states can not be treated as sequential steps and the condition in
    // following while loop needs to be changed. We can use euqals/ not equals operator to check
    // if the procedure has reached the user specified state. But there is a possibility that
    // while loop may not get the control back exaclty when the procedure is in lastStep. Proper
    // fix would be get all visited states by the procedure and then check if user speccified
    // state is in that list. Current assumption of sequential proregression of steps/ states is
    // made at multiple places so we can keep while condition below for simplicity.
    Procedure<?> proc = procExec.getProcedure(procId);
    int stepNum = proc instanceof StateMachineProcedure ?
        ((StateMachineProcedure) proc).getCurrentStateId() : 0;
    for (;;) {
      if (stepNum == lastStep) {
        break;
      }
      LOG.info("Restart " + stepNum + " exec state=" + proc);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      restartMasterProcedureExecutor(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
      // Old proc object is stale, need to get the new one after ProcedureExecutor restart
      proc = procExec.getProcedure(procId);
      stepNum = proc instanceof StateMachineProcedure ?
          ((StateMachineProcedure) proc).getCurrentStateId() : stepNum + 1;
    }

    assertEquals(expectExecRunning, procExec.isRunning());
  }

  /**
   * Run through all procedure flow states TWICE while also restarting
   * procedure executor at each step; i.e force a reread of procedure store.
   *
   *<p>It does
   * <ol><li>Execute step N - kill the executor before store update
   * <li>Restart executor/store
   * <li>Executes hook for each step twice
   * <li>Execute step N - and then save to store
   * </ol>
   *
   *<p>This is a good test for finding state that needs persisting and steps that are not
   * idempotent. Use this version of the test when the order in which flow steps are executed is
   * not start to finish; where the procedure may vary the flow steps dependent on circumstance
   * found.
   * @see #testRecoveryAndDoubleExecution(ProcedureExecutor, long, int, boolean)
   */
  public static void testRecoveryAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId, final StepHook hook)
      throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertEquals(false, procExec.isRunning());
    for (int i = 0; !procExec.isFinished(procId); ++i) {
      LOG.info("Restart " + i + " exec state=" + procExec.getProcedure(procId));
      if (hook != null) {
        assertTrue(hook.execute(i));
      }
      restartMasterProcedureExecutor(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }
    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  public static void testRecoveryAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId) throws Exception {
    testRecoveryAndDoubleExecution(procExec, procId, null);
  }

  /**
   * Hook which will be executed on each step
   */
  public interface StepHook{
    /**
     * @param step Step no. at which this will be executed
     * @return false if test should fail otherwise true
     * @throws IOException
     */
    boolean execute(int step) throws IOException;
  }

  /**
   * Execute the procedure up to "lastStep" and then the ProcedureExecutor
   * is restarted and an abort() is injected.
   * If the procedure implement abort() this should result in rollback being triggered.
   * Each rollback step is called twice, by restarting the executor after every step.
   * At the end of this call the procedure should be finished and rolledback.
   * This method assert on the procedure being terminated with an AbortException.
   */
  public static void testRollbackAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep) throws Exception {
    testRollbackAndDoubleExecution(procExec, procId, lastStep, false);
  }

  public static void testRollbackAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep, boolean waitForAsyncProcs) throws Exception {
    // Execute up to last step
    testRecoveryAndDoubleExecution(procExec, procId, lastStep, false);

    // Restart the executor and rollback the step twice
    //   rollback step N - kill before store update
    //   restart executor/store
    //   rollback step N - save on store
    InjectAbortOnLoadListener abortListener = new InjectAbortOnLoadListener(procExec);
    abortListener.addProcId(procId);
    procExec.registerListener(abortListener);
    try {
      for (int i = 0; !procExec.isFinished(procId); ++i) {
        LOG.info("Restart " + i + " rollback state: " + procExec.getProcedure(procId));
        ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
        restartMasterProcedureExecutor(procExec);
        ProcedureTestingUtility.waitProcedure(procExec, procId);
      }
    } finally {
      assertTrue(procExec.unregisterListener(abortListener));
    }

    if (waitForAsyncProcs) {
      // Sometimes there are other procedures still executing (including asynchronously spawned by
      // procId) and due to KillAndToggleBeforeStoreUpdate flag ProcedureExecutor is stopped before
      // store update. Let all pending procedures finish normally.
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
      // check 3 times to confirm that the procedure executor has not been killed
      for (int i = 0; i < 3; i++) {
        if (!procExec.isRunning()) {
          LOG.warn("ProcedureExecutor not running, may have been stopped by pending procedure due" +
            " to KillAndToggleBeforeStoreUpdate flag.");
          restartMasterProcedureExecutor(procExec);
          break;
        }
        Thread.sleep(1000);
      }
      ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    }

    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.assertIsAbortException(procExec.getResult(procId));
  }

  /**
   * Execute the procedure up to "lastStep" and then the ProcedureExecutor
   * is restarted and an abort() is injected.
   * If the procedure implement abort() this should result in rollback being triggered.
   * At the end of this call the procedure should be finished and rolledback.
   * This method assert on the procedure being terminated with an AbortException.
   */
  public static void testRollbackRetriableFailure(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep) throws Exception {
    // Execute up to last step
    testRecoveryAndDoubleExecution(procExec, procId, lastStep, false);

    // execute the rollback
    testRestartWithAbort(procExec, procId);

    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.assertIsAbortException(procExec.getResult(procId));
  }

  /**
   * Restart the ProcedureExecutor and inject an abort to the specified procedure.
   * If the procedure implement abort() this should result in rollback being triggered.
   * At the end of this call the procedure should be finished and rolledback, if abort is implemnted
   */
  public static void testRestartWithAbort(ProcedureExecutor<MasterProcedureEnv> procExec,
      long procId) throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    InjectAbortOnLoadListener abortListener = new InjectAbortOnLoadListener(procExec);
    abortListener.addProcId(procId);
    procExec.registerListener(abortListener);
    try {
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      LOG.info("Restart and rollback procId=" + procId);
      restartMasterProcedureExecutor(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    } finally {
      assertTrue(procExec.unregisterListener(abortListener));
    }
  }

  public static boolean tableExists(Connection conn, TableName tableName) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      return admin.tableExists(tableName);
    }
  }

  public static class InjectAbortOnLoadListener
      implements ProcedureExecutor.ProcedureExecutorListener {
    private final ProcedureExecutor<MasterProcedureEnv> procExec;
    private TreeSet<Long> procsToAbort = null;

    public InjectAbortOnLoadListener(final ProcedureExecutor<MasterProcedureEnv> procExec) {
      this.procExec = procExec;
    }

    public void addProcId(long procId) {
      if (procsToAbort == null) {
        procsToAbort = new TreeSet<>();
      }
      procsToAbort.add(procId);
    }

    @Override
    public void procedureLoaded(long procId) {
      if (procsToAbort != null && !procsToAbort.contains(procId)) {
        return;
      }
      procExec.abort(procId);
    }

    @Override
    public void procedureAdded(long procId) { /* no-op */ }

    @Override
    public void procedureFinished(long procId) { /* no-op */ }
  }
}
