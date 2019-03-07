/*
 *
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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.IncrementalBackupManager;
import org.apache.hadoop.hbase.backup.impl.IncrementalTableBackupClient;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner;
import org.apache.hadoop.hbase.security.HadoopSecurityEnabledUserProviderForTesting;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is only a base for other integration-level backup tests. Do not add tests here.
 * TestBackupSmallTests is where tests that don't require bring machines up/down should go All other
 * tests should have their own classes and extend this one
 */
public class TestBackupBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestBackupBase.class);

  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static HBaseTestingUtility TEST_UTIL2;
  protected static Configuration conf1 = TEST_UTIL.getConfiguration();
  protected static Configuration conf2;

  protected static TableName table1 = TableName.valueOf("table1");
  protected static HTableDescriptor table1Desc;
  protected static TableName table2 = TableName.valueOf("table2");
  protected static TableName table3 = TableName.valueOf("table3");
  protected static TableName table4 = TableName.valueOf("table4");

  protected static TableName table1_restore = TableName.valueOf("default:table1");
  protected static TableName table2_restore = TableName.valueOf("ns2:table2");
  protected static TableName table3_restore = TableName.valueOf("ns3:table3_restore");
  protected static TableName table4_restore = TableName.valueOf("ns4:table4_restore");

  protected static final int NB_ROWS_IN_BATCH = 99;
  protected static final byte[] qualName = Bytes.toBytes("q1");
  protected static final byte[] famName = Bytes.toBytes("f");

  protected static String BACKUP_ROOT_DIR = Path.SEPARATOR +"backupUT";
  protected static String BACKUP_REMOTE_ROOT_DIR = Path.SEPARATOR + "backupUT";
  protected static String provider = "defaultProvider";
  protected static boolean secure = false;

  protected static boolean autoRestoreOnFailure = true;
  protected static boolean setupIsDone = false;
  protected static boolean useSecondCluster = false;

  static class IncrementalTableBackupClientForTest extends IncrementalTableBackupClient {
    public IncrementalTableBackupClientForTest() {
    }

    public IncrementalTableBackupClientForTest(Connection conn,
        String backupId, BackupRequest request) throws IOException {
      super(conn, backupId, request);
    }

    @Override
    public void execute() throws IOException {
      // case INCREMENTAL_COPY:
      try {
        // case PREPARE_INCREMENTAL:
        failStageIf(Stage.stage_0);
        beginBackup(backupManager, backupInfo);

        failStageIf(Stage.stage_1);
        backupInfo.setPhase(BackupPhase.PREPARE_INCREMENTAL);
        LOG.debug("For incremental backup, current table set is "
            + backupManager.getIncrementalBackupTableSet());
        newTimestamps = ((IncrementalBackupManager) backupManager).getIncrBackupLogFileMap();
        // copy out the table and region info files for each table
        BackupUtils.copyTableRegionInfo(conn, backupInfo, conf);
        // convert WAL to HFiles and copy them to .tmp under BACKUP_ROOT
        convertWALsToHFiles();
        incrementalCopyHFiles(new String[] {getBulkOutputDir().toString()},
          backupInfo.getBackupRootDir());
        failStageIf(Stage.stage_2);
        // Save list of WAL files copied
        backupManager.recordWALFiles(backupInfo.getIncrBackupFileList());

        // case INCR_BACKUP_COMPLETE:
        // set overall backup status: complete. Here we make sure to complete the backup.
        // After this checkpoint, even if entering cancel process, will let the backup finished
        // Set the previousTimestampMap which is before this current log roll to the manifest.
        HashMap<TableName, HashMap<String, Long>> previousTimestampMap =
            backupManager.readLogTimestampMap();
        backupInfo.setIncrTimestampMap(previousTimestampMap);

        // The table list in backupInfo is good for both full backup and incremental backup.
        // For incremental backup, it contains the incremental backup table set.
        backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);
        failStageIf(Stage.stage_3);

        HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
            backupManager.readLogTimestampMap();

        Long newStartCode =
            BackupUtils.getMinValue(BackupUtils.getRSLogTimestampMins(newTableSetTimestampMap));
        backupManager.writeBackupStartCode(newStartCode);

        handleBulkLoad(backupInfo.getTableNames());
        failStageIf(Stage.stage_4);

        // backup complete
        completeBackup(conn, backupInfo, backupManager, BackupType.INCREMENTAL, conf);

      } catch (Exception e) {
        failBackup(conn, backupInfo, backupManager, e, "Unexpected Exception : ",
          BackupType.INCREMENTAL, conf);
        throw new IOException(e);
      }
    }
  }

  static class FullTableBackupClientForTest extends FullTableBackupClient {
    public FullTableBackupClientForTest() {
    }

    public FullTableBackupClientForTest(Connection conn, String backupId, BackupRequest request)
        throws IOException {
      super(conn, backupId, request);
    }

    @Override
    public void execute() throws IOException {
      // Get the stage ID to fail on
      try (Admin admin = conn.getAdmin()) {
        // Begin BACKUP
        beginBackup(backupManager, backupInfo);
        failStageIf(Stage.stage_0);
        String savedStartCode;
        boolean firstBackup;
        // do snapshot for full table backup
        savedStartCode = backupManager.readBackupStartCode();
        firstBackup = savedStartCode == null || Long.parseLong(savedStartCode) == 0L;
        if (firstBackup) {
          // This is our first backup. Let's put some marker to system table so that we can hold the
          // logs while we do the backup.
          backupManager.writeBackupStartCode(0L);
        }
        failStageIf(Stage.stage_1);
        // We roll log here before we do the snapshot. It is possible there is duplicate data
        // in the log that is already in the snapshot. But if we do it after the snapshot, we
        // could have data loss.
        // A better approach is to do the roll log on each RS in the same global procedure as
        // the snapshot.
        LOG.info("Execute roll log procedure for full backup ...");

        Map<String, String> props = new HashMap<>();
        props.put("backupRoot", backupInfo.getBackupRootDir());
        admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
          LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, props);
        failStageIf(Stage.stage_2);
        newTimestamps = backupManager.readRegionServerLastLogRollResult();
        if (firstBackup) {
          // Updates registered log files
          // We record ALL old WAL files as registered, because
          // this is a first full backup in the system and these
          // files are not needed for next incremental backup
          List<String> logFiles = BackupUtils.getWALFilesOlderThan(conf, newTimestamps);
          backupManager.recordWALFiles(logFiles);
        }

        // SNAPSHOT_TABLES:
        backupInfo.setPhase(BackupPhase.SNAPSHOT);
        for (TableName tableName : tableList) {
          String snapshotName =
              "snapshot_" + Long.toString(EnvironmentEdgeManager.currentTime()) + "_"
                  + tableName.getNamespaceAsString() + "_" + tableName.getQualifierAsString();

          snapshotTable(admin, tableName, snapshotName);
          backupInfo.setSnapshotName(tableName, snapshotName);
        }
        failStageIf(Stage.stage_3);
        // SNAPSHOT_COPY:
        // do snapshot copy
        LOG.debug("snapshot copy for " + backupId);
        snapshotCopy(backupInfo);
        // Updates incremental backup table set
        backupManager.addIncrementalBackupTableSet(backupInfo.getTables());

        // BACKUP_COMPLETE:
        // set overall backup status: complete. Here we make sure to complete the backup.
        // After this checkpoint, even if entering cancel process, will let the backup finished
        backupInfo.setState(BackupState.COMPLETE);
        // The table list in backupInfo is good for both full backup and incremental backup.
        // For incremental backup, it contains the incremental backup table set.
        backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);

        HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
            backupManager.readLogTimestampMap();

        Long newStartCode =
            BackupUtils.getMinValue(BackupUtils
                .getRSLogTimestampMins(newTableSetTimestampMap));
        backupManager.writeBackupStartCode(newStartCode);
        failStageIf(Stage.stage_4);
        // backup complete
        completeBackup(conn, backupInfo, backupManager, BackupType.FULL, conf);

      } catch (Exception e) {

        if(autoRestoreOnFailure) {
          failBackup(conn, backupInfo, backupManager, e, "Unexpected BackupException : ",
            BackupType.FULL, conf);
        }
        throw new IOException(e);
      }
    }
  }

  /**
   * @throws Exception if starting the mini cluster or setting up the tables fails
   */
  @Before
  public void setUp() throws Exception {
    if (setupIsDone) {
      return;
    }
    if (secure) {
      // set the always on security provider
      UserProvider.setUserProviderForTesting(TEST_UTIL.getConfiguration(),
          HadoopSecurityEnabledUserProviderForTesting.class);
      // setup configuration
      SecureTestUtil.enableSecurity(TEST_UTIL.getConfiguration());
    }
    conf1.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    BackupManager.decorateMasterConfiguration(conf1);
    BackupManager.decorateRegionServerConfiguration(conf1);
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // Set TTL for old WALs to 1 sec to enforce fast cleaning of an archived
    // WAL files
    conf1.setLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 1000);
    conf1.setLong(LogCleaner.OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC, 1000);

    // Set MultiWAL (with 2 default WAL files per RS)
    conf1.set(WALFactory.WAL_PROVIDER, provider);
    TEST_UTIL.startMiniCluster();

    if (useSecondCluster) {
      conf2 = HBaseConfiguration.create(conf1);
      conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
      TEST_UTIL2 = new HBaseTestingUtility(conf2);
      TEST_UTIL2.setZkCluster(TEST_UTIL.getZkCluster());
      TEST_UTIL2.startMiniDFSCluster(3);
      String root2 = TEST_UTIL2.getConfiguration().get("fs.defaultFS");
      Path p = new Path(new Path(root2), "/tmp/wal");
      CommonFSUtils.setWALRootDir(TEST_UTIL2.getConfiguration(), p);
      TEST_UTIL2.startMiniCluster();
    }
    conf1 = TEST_UTIL.getConfiguration();

    TEST_UTIL.startMiniMapReduceCluster();
    BACKUP_ROOT_DIR =
        new Path(new Path(TEST_UTIL.getConfiguration().get("fs.defaultFS")),
          BACKUP_ROOT_DIR).toString();
    LOG.info("ROOTDIR " + BACKUP_ROOT_DIR);
    if (useSecondCluster) {
      BACKUP_REMOTE_ROOT_DIR =
          new Path(new Path(TEST_UTIL2.getConfiguration().get("fs.defaultFS"))
          + BACKUP_REMOTE_ROOT_DIR).toString();
      LOG.info("REMOTE ROOTDIR " + BACKUP_REMOTE_ROOT_DIR);
    }
    createTables();
    populateFromMasterConfig(TEST_UTIL.getHBaseCluster().getMaster().getConfiguration(), conf1);
    setupIsDone = true;
  }

  private static void populateFromMasterConfig(Configuration masterConf, Configuration conf) {
    Iterator<Entry<String, String>> it = masterConf.iterator();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      conf.set(e.getKey(), e.getValue());
    }
  }

  /**
   * @throws Exception if deleting the archive directory or shutting down the mini cluster fails
   */
  @AfterClass
  public static void tearDown() throws Exception {
    try{
      SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    } catch (Exception e) {
    }
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
    if (useSecondCluster) {
      TEST_UTIL2.shutdownMiniCluster();
    }
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }

  Table insertIntoTable(Connection conn, TableName table, byte[] family, int id, int numRows)
      throws IOException {
    Table t = conn.getTable(table);
    Put p1;
    for (int i = 0; i < numRows; i++) {
      p1 = new Put(Bytes.toBytes("row-" + table + "-" + id + "-" + i));
      p1.addColumn(family, qualName, Bytes.toBytes("val" + i));
      t.put(p1);
    }
    return t;
  }

  protected BackupRequest createBackupRequest(BackupType type,
      List<TableName> tables, String path) {
    BackupRequest.Builder builder = new BackupRequest.Builder();
    BackupRequest request = builder.withBackupType(type)
                                    .withTableList(tables)
                                    .withTargetRootDir(path).build();
    return request;
  }

  protected String backupTables(BackupType type, List<TableName> tables, String path)
      throws IOException {
    Connection conn = null;
    BackupAdmin badmin = null;
    String backupId;
    try {
      conn = ConnectionFactory.createConnection(conf1);
      badmin = new BackupAdminImpl(conn);
      BackupRequest request = createBackupRequest(type, tables, path);
      backupId = badmin.backupTables(request);
    } finally {
      if (badmin != null) {
        badmin.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
    return backupId;
  }

  protected String fullTableBackup(List<TableName> tables) throws IOException {
    return backupTables(BackupType.FULL, tables, BACKUP_ROOT_DIR);
  }

  protected String incrementalTableBackup(List<TableName> tables) throws IOException {
    return backupTables(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
  }

  protected static void loadTable(Table table) throws Exception {
    Put p; // 100 + 1 row to t1_syncup
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      table.put(p);
    }
  }

  protected static void createTables() throws Exception {
    long tid = System.currentTimeMillis();
    table1 = TableName.valueOf("test-" + tid);
    HBaseAdmin ha = TEST_UTIL.getHBaseAdmin();

    // Create namespaces
    NamespaceDescriptor desc1 = NamespaceDescriptor.create("ns1").build();
    NamespaceDescriptor desc2 = NamespaceDescriptor.create("ns2").build();
    NamespaceDescriptor desc3 = NamespaceDescriptor.create("ns3").build();
    NamespaceDescriptor desc4 = NamespaceDescriptor.create("ns4").build();

    ha.createNamespace(desc1);
    ha.createNamespace(desc2);
    ha.createNamespace(desc3);
    ha.createNamespace(desc4);

    HTableDescriptor desc = new HTableDescriptor(table1);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    desc.addFamily(fam);
    ha.createTable(desc);
    table1Desc = desc;
    Connection conn = ConnectionFactory.createConnection(conf1);
    Table table = conn.getTable(table1);
    loadTable(table);
    table.close();
    table2 = TableName.valueOf("ns2:test-" + tid + 1);
    desc = new HTableDescriptor(table2);
    desc.addFamily(fam);
    ha.createTable(desc);
    table = conn.getTable(table2);
    loadTable(table);
    table.close();
    table3 = TableName.valueOf("ns3:test-" + tid + 2);
    table = TEST_UTIL.createTable(table3, famName);
    table.close();
    table4 = TableName.valueOf("ns4:test-" + tid + 3);
    table = TEST_UTIL.createTable(table4, famName);
    table.close();
    ha.close();
    conn.close();
  }

  protected boolean checkSucceeded(String backupId) throws IOException {
    BackupInfo status = getBackupInfo(backupId);

    if (status == null) {
      return false;
    }

    return status.getState() == BackupState.COMPLETE;
  }

  protected boolean checkFailed(String backupId) throws IOException {
    BackupInfo status = getBackupInfo(backupId);

    if (status == null) {
      return false;
    }

    return status.getState() == BackupState.FAILED;
  }

  private BackupInfo getBackupInfo(String backupId) throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      BackupInfo status = table.readBackupInfo(backupId);
      return status;
    }
  }

  protected BackupAdmin getBackupAdmin() throws IOException {
    return new BackupAdminImpl(TEST_UTIL.getConnection());
  }

  /**
   * Helper method
   */
  protected List<TableName> toList(String... args) {
    List<TableName> ret = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      ret.add(TableName.valueOf(args[i]));
    }
    return ret;
  }

  protected void dumpBackupDir() throws IOException {
    // Dump Backup Dir
    FileSystem fs = FileSystem.get(conf1);
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(BACKUP_ROOT_DIR), true);
    while (it.hasNext()) {
      LOG.debug(Objects.toString(it.next().getPath()));
    }
  }
}
