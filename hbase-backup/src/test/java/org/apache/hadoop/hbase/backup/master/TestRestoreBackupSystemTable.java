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
package org.apache.hadoop.hbase.backup.master;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestRestoreBackupSystemTable {
  private static final String BACKUP_ROOT = "root";
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster();
  }

  @Test
  public void itRestoresFromSnapshot() throws Exception {
    BackupSystemTable table = new BackupSystemTable(UTIL.getConnection());
    Set<TableName> tables = new HashSet<>();

    tables.add(TableName.valueOf("test1"));
    tables.add(TableName.valueOf("test2"));
    tables.add(TableName.valueOf("test3"));

    Map<String, Long> rsTimestampMap = new HashMap<>();
    rsTimestampMap.put("rs1:100", 100L);
    rsTimestampMap.put("rs2:100", 101L);
    rsTimestampMap.put("rs3:100", 103L);

    table.writeRegionServerLogTimestamp(tables, rsTimestampMap, BACKUP_ROOT);
    BackupSystemTable.snapshot(UTIL.getConnection());

    Admin admin = UTIL.getAdmin();
    TableName backupSystemTn = BackupSystemTable.getTableName(UTIL.getConfiguration());
    admin.disableTable(backupSystemTn);
    admin.truncateTable(backupSystemTn, true);

    BackupSystemTable.restoreFromSnapshot(UTIL.getConnection());
    Map<TableName, Map<String, Long>> results = table.readLogTimestampMap(BACKUP_ROOT);

    assertEquals(results.size(), tables.size());

    for (TableName tableName : tables) {
      Map<String, Long> resultMap = results.get(tableName);
      assertEquals(resultMap, rsTimestampMap);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
