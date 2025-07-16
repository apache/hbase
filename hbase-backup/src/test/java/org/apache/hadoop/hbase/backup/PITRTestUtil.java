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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_PITR_BACKUP_PATH;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_MAPPING;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TO_DATETIME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class PITRTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(PITRTestUtil.class);
  private static final int DEFAULT_WAIT_FOR_REPLICATION_MS = 30_000;

  private PITRTestUtil() {
    // Utility class
  }

  public static String[] buildPITRArgs(TableName[] sourceTables, TableName[] targetTables,
    long endTime, String backupRootDir) {
    String sourceTableNames =
      Arrays.stream(sourceTables).map(TableName::getNameAsString).collect(Collectors.joining(","));
    String targetTableNames =
      Arrays.stream(targetTables).map(TableName::getNameAsString).collect(Collectors.joining(","));

    List<String> args = new ArrayList<>();
    args.add("-" + OPTION_TABLE);
    args.add(sourceTableNames);
    args.add("-" + OPTION_TABLE_MAPPING);
    args.add(targetTableNames);
    args.add("-" + OPTION_TO_DATETIME);
    args.add(String.valueOf(endTime));

    if (backupRootDir != null) {
      args.add("-" + OPTION_PITR_BACKUP_PATH);
      args.add(backupRootDir);
    }

    return args.toArray(new String[0]);
  }

  public static String[] buildBackupArgs(String backupType, TableName[] tables,
    boolean continuousEnabled, String backupRootDir) {
    String tableNames =
      Arrays.stream(tables).map(TableName::getNameAsString).collect(Collectors.joining(","));

    List<String> args = new ArrayList<>(
      Arrays.asList("create", backupType, backupRootDir, "-" + OPTION_TABLE, tableNames));

    if (continuousEnabled) {
      args.add("-" + OPTION_ENABLE_CONTINUOUS_BACKUP);
    }

    return args.toArray(new String[0]);
  }

  public static void loadRandomData(HBaseTestingUtil testUtil, TableName tableName, byte[] family,
    int totalRows) throws IOException {
    try (Table table = testUtil.getConnection().getTable(tableName)) {
      testUtil.loadRandomRows(table, family, 32, totalRows);
    }
  }

  public static void waitForReplication() {
    try {
      LOG.info("Waiting for replication to complete for {} ms", DEFAULT_WAIT_FOR_REPLICATION_MS);
      Thread.sleep(DEFAULT_WAIT_FOR_REPLICATION_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for replication", e);
    }
  }

  public static int getRowCount(HBaseTestingUtil testUtil, TableName tableName) throws IOException {
    try (Table table = testUtil.getConnection().getTable(tableName)) {
      return HBaseTestingUtil.countRows(table);
    }
  }
}
