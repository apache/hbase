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
package org.apache.hadoop.hbase.backup.replication;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.WALS_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_BACKUP_MAX_WAL_SIZE;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_BACKUP_ROOT_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_PEER_UUID;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INITIAL_DELAY;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INTERVAL;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.DATE_FORMAT;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.WAL_FILE_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestContinuousBackupReplicationEndpoint {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestContinuousBackupReplicationEndpoint.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestContinuousBackupReplicationEndpoint.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin;

  private final String replicationEndpoint = ContinuousBackupReplicationEndpoint.class.getName();
  private static final String CF_NAME = "cf";
  static FileSystem fs = null;
  static Path root;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set the configuration properties as required
    conf.set(REPLICATION_CLUSTER_ID, "clusterId1");

    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster(3);
    fs = FileSystem.get(conf);
    root = TEST_UTIL.getDataTestDirOnTestFS();
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (fs != null) {
      fs.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testWALBackup() throws IOException {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    String peerId = "peerId";

    createTable(tableName);

    Path backupRootDir = new Path(root, methodName);
    fs.mkdirs(backupRootDir);

    Map<TableName, List<String>> tableMap = new HashMap<>();
    tableMap.put(tableName, new ArrayList<>());

    addReplicationPeer(peerId, backupRootDir, tableMap);

    loadRandomData(tableName, 100);
    assertEquals(100, getRowCount(tableName));

    waitForReplication(15000);
    deleteReplicationPeer(peerId);

    verifyBackup(backupRootDir.toString(), Map.of(tableName, 100));

    deleteTable(tableName);
  }

  @Test
  public void testMultiTableWALBackup() throws IOException {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName table1 = TableName.valueOf("table_" + methodName + "1");
    TableName table2 = TableName.valueOf("table_" + methodName + "2");
    TableName table3 = TableName.valueOf("table_" + methodName + "3");
    String peerId = "peerMulti";

    for (TableName table : List.of(table1, table2, table3)) {
      createTable(table);
    }

    Path backupRootDir = new Path(root, methodName);
    fs.mkdirs(backupRootDir);

    Map<TableName, List<String>> initialTableMap = new HashMap<>();
    initialTableMap.put(table1, new ArrayList<>());
    initialTableMap.put(table2, new ArrayList<>());

    addReplicationPeer(peerId, backupRootDir, initialTableMap);

    for (TableName table : List.of(table1, table2, table3)) {
      loadRandomData(table, 50);
      assertEquals(50, getRowCount(table));
    }

    waitForReplication(15000);

    // Update the Replication Peer to Include table3
    admin.updateReplicationPeerConfig(peerId,
      ReplicationPeerConfig.newBuilder(admin.getReplicationPeerConfig(peerId))
        .setTableCFsMap(
          Map.of(table1, new ArrayList<>(), table2, new ArrayList<>(), table3, new ArrayList<>()))
        .build());

    for (TableName table : List.of(table1, table2, table3)) {
      loadRandomData(table, 50);
      assertEquals(100, getRowCount(table));
    }

    waitForReplication(15000);
    deleteReplicationPeer(peerId);

    verifyBackup(backupRootDir.toString(), Map.of(table1, 100, table2, 100, table3, 50));

    for (TableName table : List.of(table1, table2, table3)) {
      deleteTable(table);
    }
  }

  @Test
  public void testWALBackupWithPeerRestart() throws IOException, InterruptedException {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    String peerId = "peerId";

    createTable(tableName);

    Path backupRootDir = new Path(root, methodName);
    fs.mkdirs(backupRootDir);

    Map<TableName, List<String>> tableMap = new HashMap<>();
    tableMap.put(tableName, new ArrayList<>());

    addReplicationPeer(peerId, backupRootDir, tableMap);

    AtomicBoolean stopLoading = new AtomicBoolean(false);

    // Start a separate thread to load data continuously
    Thread dataLoaderThread = new Thread(() -> {
      try {
        while (!stopLoading.get()) {
          loadRandomData(tableName, 10);
          Thread.sleep(1000); // Simulate delay
        }
      } catch (Exception e) {
        LOG.error("Data loading thread encountered an error", e);
      }
    });

    dataLoaderThread.start();

    // Main thread enables and disables replication peer
    try {
      for (int i = 0; i < 5; i++) {
        LOG.info("Disabling replication peer...");
        admin.disableReplicationPeer(peerId);
        Thread.sleep(2000);

        LOG.info("Enabling replication peer...");
        admin.enableReplicationPeer(peerId);
        Thread.sleep(2000);
      }
    } finally {
      stopLoading.set(true); // Stop the data loader thread
      dataLoaderThread.join();
    }

    waitForReplication(20000);
    deleteReplicationPeer(peerId);

    verifyBackup(backupRootDir.toString(), Map.of(tableName, getRowCount(tableName)));

    deleteTable(tableName);
  }

  @Test
  public void testDayWiseWALBackup() throws IOException {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    String peerId = "peerId";

    createTable(tableName);

    Path backupRootDir = new Path(root, methodName);
    fs.mkdirs(backupRootDir);

    Map<TableName, List<String>> tableMap = new HashMap<>();
    tableMap.put(tableName, new ArrayList<>());

    addReplicationPeer(peerId, backupRootDir, tableMap);

    // Mock system time using ManualEnvironmentEdge
    ManualEnvironmentEdge manualEdge = new ManualEnvironmentEdge();
    EnvironmentEdgeManagerTestHelper.injectEdge(manualEdge);

    long currentTime = System.currentTimeMillis();
    long oneDayBackTime = currentTime - ONE_DAY_IN_MILLISECONDS;

    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    String expectedPrevDayDir = dateFormat.format(new Date(oneDayBackTime));
    String expectedCurrentDayDir = dateFormat.format(new Date(currentTime));

    manualEdge.setValue(oneDayBackTime);
    loadRandomData(tableName, 100);
    assertEquals(100, getRowCount(tableName));

    manualEdge.setValue(currentTime);
    loadRandomData(tableName, 100);
    assertEquals(200, getRowCount(tableName));

    // Reset time mocking
    EnvironmentEdgeManagerTestHelper.reset();

    waitForReplication(15000);
    deleteReplicationPeer(peerId);

    verifyBackup(backupRootDir.toString(), Map.of(tableName, 200));

    // Verify that WALs are stored in two directories, one for each day
    Path walDir = new Path(backupRootDir, WALS_DIR);
    Set<String> walDirectories = new HashSet<>();

    FileStatus[] fileStatuses = fs.listStatus(walDir);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String dirName = fileStatus.getPath().getName();
        walDirectories.add(dirName);
      }
    }

    assertEquals("WALs should be stored in exactly two directories", 2, walDirectories.size());
    assertTrue("Expected previous day's WAL directory missing",
      walDirectories.contains(expectedPrevDayDir));
    assertTrue("Expected current day's WAL directory missing",
      walDirectories.contains(expectedCurrentDayDir));

    deleteTable(tableName);
  }

  private void createTable(TableName tableName) throws IOException {
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(CF_NAME)).setScope(1).build();
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(columnFamilyDescriptor).build();

    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor);
    }
  }

  private void deleteTable(TableName tableName) throws IOException {
    admin.disableTable(tableName);
    admin.truncateTable(tableName, false);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  private void addReplicationPeer(String peerId, Path backupRootDir,
    Map<TableName, List<String>> tableMap) throws IOException {
    Map<String, String> additionalArgs = new HashMap<>();
    additionalArgs.put(CONF_PEER_UUID, UUID.randomUUID().toString());
    additionalArgs.put(CONF_BACKUP_ROOT_DIR, backupRootDir.toString());
    additionalArgs.put(CONF_BACKUP_MAX_WAL_SIZE, "10240");
    additionalArgs.put(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY, "10");
    additionalArgs.put(CONF_STAGED_WAL_FLUSH_INTERVAL, "10");

    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setReplicationEndpointImpl(replicationEndpoint).setReplicateAllUserTables(false)
      .setTableCFsMap(tableMap).putAllConfiguration(additionalArgs).build();

    admin.addReplicationPeer(peerId, peerConfig);
  }

  private void deleteReplicationPeer(String peerId) throws IOException {
    admin.disableReplicationPeer(peerId);
    admin.removeReplicationPeer(peerId);
  }

  private void loadRandomData(TableName tableName, int totalRows) throws IOException {
    int rowSize = 32;
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      TEST_UTIL.loadRandomRows(table, Bytes.toBytes(CF_NAME), rowSize, totalRows);
    }
  }

  private void waitForReplication(int durationInMillis) {
    LOG.info("Waiting for replication to complete for {} ms", durationInMillis);
    try {
      Thread.sleep(durationInMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread was interrupted while waiting", e);
    }
  }

  /**
   * Verifies the backup process by: 1. Checking whether any WAL (Write-Ahead Log) files were
   * generated in the backup directory. 2. Replaying the WAL files to restore data and check
   * consistency by verifying that the restored data matches the expected row count for each table.
   */
  private void verifyBackup(String backupRootDir, Map<TableName, Integer> tablesWithExpectedRows)
    throws IOException {
    verifyWALBackup(backupRootDir);

    for (Map.Entry<TableName, Integer> entry : tablesWithExpectedRows.entrySet()) {
      TableName tableName = entry.getKey();
      int expectedRows = entry.getValue();

      admin.disableTable(tableName);
      admin.truncateTable(tableName, false);
      assertEquals(0, getRowCount(tableName));

      replayWALs(new Path(backupRootDir, WALS_DIR).toString(), tableName);

      assertEquals(expectedRows, getRowCount(tableName));
    }
  }

  private void verifyWALBackup(String backupRootDir) throws IOException {
    Path walDir = new Path(backupRootDir, WALS_DIR);
    assertTrue("WAL directory does not exist!", fs.exists(walDir));

    RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(walDir, true);
    List<Path> walFiles = new ArrayList<>();

    while (fileStatusIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIterator.next();
      Path filePath = fileStatus.getPath();

      // Check if the file starts with the expected WAL prefix
      if (!fileStatus.isDirectory() && filePath.getName().startsWith(WAL_FILE_PREFIX)) {
        walFiles.add(filePath);
      }
    }

    assertNotNull("No WAL files found!", walFiles);
    assertFalse("Expected some WAL files but found none!", walFiles.isEmpty());
  }

  private void replayWALs(String walDir, TableName tableName) {
    WALPlayer player = new WALPlayer();
    try {
      assertEquals(0, ToolRunner.run(TEST_UTIL.getConfiguration(), player,
        new String[] { walDir, tableName.getQualifierAsString() }));
    } catch (Exception e) {
      fail("Failed to replay WALs properly: " + e.getMessage());
    }
  }

  private int getRowCount(TableName tableName) throws IOException {
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      return HBaseTestingUtil.countRows(table);
    }
  }
}
