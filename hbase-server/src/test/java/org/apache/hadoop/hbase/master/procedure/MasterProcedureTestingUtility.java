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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MasterProcedureTestingUtility {
  private static final Log LOG = LogFactory.getLog(MasterProcedureTestingUtility.class);

  private MasterProcedureTestingUtility() {
  }

  public static HTableDescriptor createHTD(final TableName tableName, final String... family) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (int i = 0; i < family.length; ++i) {
      htd.addFamily(new HColumnDescriptor(family[i]));
    }
    return htd;
  }

  public static HRegionInfo[] createTable(final ProcedureExecutor<MasterProcedureEnv> procExec,
      final TableName tableName, final byte[][] splitKeys, String... family) throws IOException {
    HTableDescriptor htd = createHTD(tableName, family);
    return createTable(procExec, htd, splitKeys);
  }

  public static HRegionInfo[] createTable(final ProcedureExecutor<MasterProcedureEnv> procExec,
      HTableDescriptor htd, final byte[][] splitKeys) throws IOException {
    HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, splitKeys);
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));
    return regions;
  }

  public static void validateTableCreation(final HMaster master, final TableName tableName,
      final HRegionInfo[] regions, String... family) throws IOException {
    validateTableCreation(master, tableName, regions, true, family);
  }

  public static void validateTableCreation(final HMaster master, final TableName tableName,
      final HRegionInfo[] regions, boolean hasFamilyDirs, String... family) throws IOException {
    // check filesystem
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir = FSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    assertTrue(fs.exists(tableDir));
    FSUtils.logFileSystemState(fs, tableDir, LOG);
    List<Path> allRegionDirs = FSUtils.getRegionDirs(fs, tableDir);
    for (int i = 0; i < regions.length; ++i) {
      Path regionDir = new Path(tableDir, regions[i].getEncodedName());
      assertTrue(regions[i] + " region dir does not exist", fs.exists(regionDir));
      assertTrue(allRegionDirs.remove(regionDir));
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
    assertTrue("found extraneous regions: " + allRegionDirs, allRegionDirs.isEmpty());

    // check meta
    assertTrue(MetaTableAccessor.tableExists(master.getConnection(), tableName));
    assertEquals(regions.length, countMetaRegions(master, tableName));

    // check htd
    HTableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue("table descriptor not found", htd != null);
    for (int i = 0; i < family.length; ++i) {
      assertTrue("family not found " + family[i], htd.getFamily(Bytes.toBytes(family[i])) != null);
    }
    assertEquals(family.length, htd.getFamilies().size());
  }

  public static void validateTableDeletion(final HMaster master, final TableName tableName,
      final HRegionInfo[] regions, String... family) throws IOException {
    // check filesystem
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir = FSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    assertFalse(fs.exists(tableDir));

    // check meta
    assertFalse(MetaTableAccessor.tableExists(master.getConnection(), tableName));
    assertEquals(0, countMetaRegions(master, tableName));

    // check htd
    assertTrue("found htd of deleted table",
      master.getTableDescriptors().get(tableName) == null);
  }

  private static int countMetaRegions(final HMaster master, final TableName tableName)
      throws IOException {
    final AtomicInteger actualRegCount = new AtomicInteger(0);
    final MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result rowResult) throws IOException {
        RegionLocations list = MetaTableAccessor.getRegionLocations(rowResult);
        if (list == null) {
          LOG.warn("No serialized HRegionInfo in " + rowResult);
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
          if (serverName != null && serverName.getHostAndPort() != null) {
            actualRegCount.incrementAndGet();
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(master.getConnection(), visitor, tableName);
    return actualRegCount.get();
  }

  public static void validateTableIsEnabled(final HMaster master, final TableName tableName)
      throws IOException {
    TableStateManager tsm = master.getAssignmentManager().getTableStateManager();
    assertTrue(tsm.isTableState(tableName, ZooKeeperProtos.Table.State.ENABLED));
  }

  public static void validateTableIsDisabled(final HMaster master, final TableName tableName)
      throws IOException {
    TableStateManager tsm = master.getAssignmentManager().getTableStateManager();
    assertTrue(tsm.isTableState(tableName, ZooKeeperProtos.Table.State.DISABLED));
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
  public static <TState> void testRecoveryAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int numSteps, final TState[] states) throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertEquals(false, procExec.isRunning());
    for (int i = 0; i < numSteps; ++i) {
      LOG.info("Restart "+ i +" exec state: " + states[i]);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }
    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
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
   * idempotent. Use this version of the test when the order in which flow steps are executed is
   * not start to finish; where the procedure may vary the flow steps dependent on circumstance
   * found.
   * @see #testRecoveryAndDoubleExecution(ProcedureExecutor, long, int, Object[])
   */
  public static <TState> void testRecoveryAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId)
  throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertEquals(false, procExec.isRunning());
    while (!procExec.isFinished(procId)) {
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }
    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  public static <TState> void testRollbackAndDoubleExecution(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep, final TState[] states) throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Restart the executor and execute the step twice
    //   execute step N - kill before store update
    //   restart executor/store
    //   execute step N - save on store
    for (int i = 0; i < lastStep; ++i) {
      LOG.info("Restart "+ i +" exec state: " + states[i]);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }

    // Restart the executor and rollback the step twice
    //   rollback step N - kill before store update
    //   restart executor/store
    //   rollback step N - save on store
    InjectAbortOnLoadListener abortListener = new InjectAbortOnLoadListener(procExec);
    procExec.registerListener(abortListener);
    try {
      for (int i = lastStep + 1; i >= 0; --i) {
        LOG.info("Restart " + i +" rollback state: "+ states[i]);
        ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
        ProcedureTestingUtility.restart(procExec);
        ProcedureTestingUtility.waitProcedure(procExec, procId);
      }
    } finally {
      assertTrue(procExec.unregisterListener(abortListener));
    }

    ProcedureTestingUtility.assertIsAbortException(procExec.getResult(procId));
  }

  public static <TState> void testRollbackAndDoubleExecutionAfterPONR(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep, final TState[] states) throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Restart the executor and execute the step twice
    //   execute step N - kill before store update
    //   restart executor/store
    //   execute step N - save on store
    for (int i = 0; i < lastStep; ++i) {
      LOG.info("Restart "+ i +" exec state: " + states[i]);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }

    // try to inject the abort
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    InjectAbortOnLoadListener abortListener = new InjectAbortOnLoadListener(procExec);
    procExec.registerListener(abortListener);
    try {
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      LOG.info("Restart and execute");
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    } finally {
      assertTrue(procExec.unregisterListener(abortListener));
    }

    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  public static <TState> void testRollbackRetriableFailure(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final long procId,
      final int lastStep, final TState[] states) throws Exception {
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Restart the executor and execute the step twice
    //   execute step N - kill before store update
    //   restart executor/store
    //   execute step N - save on store
    for (int i = 0; i < lastStep; ++i) {
      LOG.info("Restart "+ i +" exec state: " + states[i]);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }

    // execute the rollback
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    InjectAbortOnLoadListener abortListener = new InjectAbortOnLoadListener(procExec);
    procExec.registerListener(abortListener);
    try {
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      LOG.info("Restart and rollback");
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    } finally {
      assertTrue(procExec.unregisterListener(abortListener));
    }

    ProcedureTestingUtility.assertIsAbortException(procExec.getResult(procId));
  }

  public static void testRestartWithAbort(ProcedureExecutor<MasterProcedureEnv> procExec,
      long procId) throws Exception {
    InjectAbortOnLoadListener abortListener = new InjectAbortOnLoadListener(procExec);
    abortListener.addProcId(procId);
    procExec.registerListener(abortListener);
    try {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    } finally {
      assertTrue(procExec.unregisterListener(abortListener));
    }
  }

  public static void validateColumnFamilyAddition(final HMaster master, final TableName tableName,
      final String family) throws IOException {
    HTableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue(htd != null);
    assertTrue(htd.hasFamily(family.getBytes()));
  }

  public static void validateColumnFamilyDeletion(final HMaster master, final TableName tableName,
      final String family) throws IOException {
    // verify htd
    HTableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue(htd != null);
    assertFalse(htd.hasFamily(family.getBytes()));

    // verify fs
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir = FSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    for (Path regionDir: FSUtils.getRegionDirs(fs, tableDir)) {
      final Path familyDir = new Path(regionDir, family);
      assertFalse(family + " family dir should not exist", fs.exists(familyDir));
    }
  }

  public static void validateColumnFamilyModification(final HMaster master,
      final TableName tableName, final String family, HColumnDescriptor columnDescriptor)
      throws IOException {
    HTableDescriptor htd = master.getTableDescriptors().get(tableName);
    assertTrue(htd != null);

    HColumnDescriptor hcfd = htd.getFamily(family.getBytes());
    assertTrue(hcfd.equals(columnDescriptor));
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
      put.add(family, q, value);
    }
    return put;
  }

  public static long generateNonceGroup(final HMaster master) {
    return master.getConnection().getNonceGenerator().getNonceGroup();
  }

  public static long generateNonce(final HMaster master) {
    return master.getConnection().getNonceGenerator().newNonce();
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
        procsToAbort = new TreeSet<Long>();
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
