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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.BackupInfo.BackupState.COMPLETE;
import static org.apache.hadoop.hbase.backup.BackupTestUtil.enableBackup;
import static org.apache.hadoop.hbase.backup.BackupTestUtil.verifyBackup;
import static org.apache.hadoop.hbase.backup.BackupType.FULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestBackupRestoreWithModifications {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBackupRestoreWithModifications.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupRestoreWithModifications.class);

  @Parameterized.Parameters(name = "{index}: useBulkLoad={0}")
  public static Iterable<Object[]> data() {
    return HBaseCommonTestingUtil.BOOLEAN_PARAMETERIZED;
  }

  @Parameterized.Parameter(0)
  public boolean useBulkLoad;

  private TableName sourceTable;
  private TableName targetTable;

  private List<TableName> allTables;
  private static TestingHBaseCluster cluster;
  private static final Path BACKUP_ROOT_DIR = new Path("backupIT");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("0");

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    enableBackup(conf);
    cluster = TestingHBaseCluster.create(TestingHBaseClusterOption.builder().conf(conf).build());
    cluster.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.stop();
  }

  @Before
  public void setUp() throws Exception {
    sourceTable = TableName.valueOf("table-" + useBulkLoad);
    targetTable = TableName.valueOf("another-table-" + useBulkLoad);
    allTables = Arrays.asList(sourceTable, targetTable);
    createTable(sourceTable);
    createTable(targetTable);
  }

  @Test
  public void testModificationsOnTable() throws Exception {
    Instant timestamp = Instant.now();

    // load some data
    load(sourceTable, timestamp, "data");

    String backupId = backup(FULL, allTables);
    BackupInfo backupInfo = verifyBackup(cluster.getConf(), backupId, FULL, COMPLETE);
    assertTrue(backupInfo.getTables().contains(sourceTable));

    restore(backupId, sourceTable, targetTable);
    validateDataEquals(sourceTable, "data");
    validateDataEquals(targetTable, "data");

    // load new data on the same timestamp
    load(sourceTable, timestamp, "changed_data");

    backupId = backup(FULL, allTables);
    backupInfo = verifyBackup(cluster.getConf(), backupId, FULL, COMPLETE);
    assertTrue(backupInfo.getTables().contains(sourceTable));

    restore(backupId, sourceTable, targetTable);
    validateDataEquals(sourceTable, "changed_data");
    validateDataEquals(targetTable, "changed_data");
  }

  private void createTable(TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY));
    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf());
      Admin admin = connection.getAdmin()) {
      admin.createTable(builder.build());
    }
  }

  private void load(TableName tableName, Instant timestamp, String data) throws IOException {
    if (useBulkLoad) {
      hFileBulkLoad(tableName, timestamp, data);
    } else {
      putLoad(tableName, timestamp, data);
    }
  }

  private void putLoad(TableName tableName, Instant timestamp, String data) throws IOException {
    LOG.info("Writing new data to HBase using normal Puts: {}", data);
    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf())) {
      Table table = connection.getTable(sourceTable);
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Put put = new Put(Bytes.toBytes(i), timestamp.toEpochMilli());
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("data"), Bytes.toBytes(data));
        puts.add(put);

        if (i % 100 == 0) {
          table.put(puts);
          puts.clear();
        }
      }
      if (!puts.isEmpty()) {
        table.put(puts);
      }
      connection.getAdmin().flush(tableName);
    }
  }

  private void hFileBulkLoad(TableName tableName, Instant timestamp, String data)
    throws IOException {
    FileSystem fs = FileSystem.get(cluster.getConf());
    LOG.info("Writing new data to HBase using BulkLoad: {}", data);
    // HFiles require this strict directory structure to allow to load them
    Path hFileRootPath = new Path("/tmp/hfiles_" + UUID.randomUUID());
    fs.mkdirs(hFileRootPath);
    Path hFileFamilyPath = new Path(hFileRootPath, Bytes.toString(COLUMN_FAMILY));
    fs.mkdirs(hFileFamilyPath);
    try (HFile.Writer writer = HFile.getWriterFactoryNoCache(cluster.getConf())
      .withPath(fs, new Path(hFileFamilyPath, "hfile_" + UUID.randomUUID()))
      .withFileContext(new HFileContextBuilder().withTableName(tableName.toBytes())
        .withColumnFamily(COLUMN_FAMILY).build())
      .create()) {
      for (int i = 0; i < 10; i++) {
        writer.append(new KeyValue(Bytes.toBytes(i), COLUMN_FAMILY, Bytes.toBytes("data"),
          timestamp.toEpochMilli(), Bytes.toBytes(data)));
      }
    }
    Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> result =
      BulkLoadHFiles.create(cluster.getConf()).bulkLoad(tableName, hFileRootPath);
    assertFalse(result.isEmpty());
  }

  private String backup(BackupType backupType, List<TableName> tables) throws IOException {
    LOG.info("Creating the backup ...");

    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf());
      BackupAdmin backupAdmin = new BackupAdminImpl(connection)) {
      BackupRequest backupRequest =
        new BackupRequest.Builder().withTargetRootDir(BACKUP_ROOT_DIR.toString())
          .withTableList(new ArrayList<>(tables)).withBackupType(backupType).build();
      return backupAdmin.backupTables(backupRequest);
    }

  }

  private void restore(String backupId, TableName sourceTableName, TableName targetTableName)
    throws IOException {
    LOG.info("Restoring data ...");
    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf());
      BackupAdmin backupAdmin = new BackupAdminImpl(connection)) {
      RestoreRequest restoreRequest = new RestoreRequest.Builder().withBackupId(backupId)
        .withBackupRootDir(BACKUP_ROOT_DIR.toString()).withOverwrite(true)
        .withFromTables(new TableName[] { sourceTableName })
        .withToTables(new TableName[] { targetTableName }).build();
      backupAdmin.restore(restoreRequest);
    }
  }

  private void validateDataEquals(TableName tableName, String expectedData) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf());
      Table table = connection.getTable(tableName)) {
      Scan scan = new Scan();
      scan.readAllVersions();
      scan.setRaw(true);
      scan.setBatch(100);

      for (Result sourceResult : table.getScanner(scan)) {
        List<Cell> sourceCells = sourceResult.listCells();
        for (Cell cell : sourceCells) {
          assertEquals(expectedData, Bytes.toStringBinary(cell.getValueArray(),
            cell.getValueOffset(), cell.getValueLength()));
        }
      }
    }
  }

}
