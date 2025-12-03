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
import static org.apache.hadoop.hbase.backup.BackupType.INCREMENTAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
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
public class TestBackupRestoreOnEmptyEnvironment {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBackupRestoreOnEmptyEnvironment.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupRestoreOnEmptyEnvironment.class);

  @Parameterized.Parameters(name = "{index}: restoreToOtherTable={0}")
  public static Iterable<Object[]> data() {
    return HBaseCommonTestingUtil.BOOLEAN_PARAMETERIZED;
  }

  @Parameterized.Parameter(0)
  public boolean restoreToOtherTable;
  private TableName sourceTable;
  private TableName targetTable;

  private static TestingHBaseCluster cluster;
  private static Path BACKUP_ROOT_DIR;
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("0");

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    enableBackup(conf);
    cluster = TestingHBaseCluster.create(TestingHBaseClusterOption.builder().conf(conf).build());
    cluster.start();
    BACKUP_ROOT_DIR = new Path(new Path(conf.get("fs.defaultFS")), new Path("/backupIT"));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.stop();
  }

  @Before
  public void setUp() throws Exception {
    sourceTable = TableName.valueOf("table");
    targetTable = TableName.valueOf("another-table");
    createTable(sourceTable);
    createTable(targetTable);
  }

  @After
  public void removeTables() throws Exception {
    deleteTables();
  }

  @Test
  public void testRestoreToCorrectTable() throws Exception {
    Instant timestamp = Instant.now().minusSeconds(10);

    // load some data
    putLoad(sourceTable, timestamp, "data");

    String backupId = backup(FULL, Collections.singletonList(sourceTable));
    BackupInfo backupInfo = verifyBackup(cluster.getConf(), backupId, FULL, COMPLETE);
    assertTrue(backupInfo.getTables().contains(sourceTable));

    LOG.info("Deleting the tables before restore ...");
    deleteTables();

    if (restoreToOtherTable) {
      restore(backupId, sourceTable, targetTable);
      validateDataEquals(targetTable, "data");
    } else {
      restore(backupId, sourceTable, sourceTable);
      validateDataEquals(sourceTable, "data");
    }

  }

  @Test
  public void testRestoreCorrectTableForIncremental() throws Exception {
    Instant timestamp = Instant.now().minusSeconds(10);

    // load some data
    putLoad(sourceTable, timestamp, "data");

    String backupId = backup(FULL, Collections.singletonList(sourceTable));
    verifyBackup(cluster.getConf(), backupId, FULL, COMPLETE);

    // some incremental data
    putLoad(sourceTable, timestamp.plusMillis(1), "new_data");

    String backupId2 = backup(INCREMENTAL, Collections.singletonList(sourceTable));
    verifyBackup(cluster.getConf(), backupId2, INCREMENTAL, COMPLETE);

    LOG.info("Deleting the tables before restore ...");
    deleteTables();

    if (restoreToOtherTable) {
      restore(backupId2, sourceTable, targetTable);
      validateDataEquals(targetTable, "new_data");
    } else {
      restore(backupId2, sourceTable, sourceTable);
      validateDataEquals(sourceTable, "new_data");
    }

  }

  private void createTable(TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY));
    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf());
      Admin admin = connection.getAdmin()) {
      admin.createTable(builder.build());
    }
  }

  private void deleteTables() throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(cluster.getConf());
      Admin admin = connection.getAdmin()) {
      for (TableName table : Arrays.asList(sourceTable, targetTable)) {
        if (admin.tableExists(table)) {
          admin.disableTable(table);
          admin.deleteTable(table);
        }
      }
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
