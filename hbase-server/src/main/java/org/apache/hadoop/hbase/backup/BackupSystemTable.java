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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupHandler.BACKUPSTATUS;
import org.apache.hadoop.hbase.backup.BackupUtil.BackupCompleteData;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 * This class provides 'hbase:backup' table API
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupSystemTable {

  private static final Log LOG = LogFactory.getLog(BackupSystemTable.class);
  private final static String TABLE_NAMESPACE = "hbase";
  private final static String TABLE_NAME = "backup";
  private final static TableName tableName = TableName.valueOf(TABLE_NAMESPACE, TABLE_NAME);
  public final static byte[] familyName = "f".getBytes();

  // Connection to HBase cluster
  private static Connection connection;
  // Cluster configuration
  private static Configuration config;
  // singleton
  private static BackupSystemTable table;

  /**
   * Get instance by a given configuration
   * @param conf - HBase configuration
   * @return instance of BackupSystemTable
   * @throws IOException exception
   */
  public synchronized static BackupSystemTable getTable(Configuration conf) throws IOException {
    if (connection == null) {
      connection = ConnectionFactory.createConnection(conf);
      config = conf;
      // Verify hbase:system exists
      createSystemTableIfNotExists();
      table = new BackupSystemTable();
    }
    return table;
  }

  /**
   * TODO: refactor
   * @throws IOException exception
   */
  public static void close() throws IOException {
    connection.close();
    table = null;
  }

  /**
   * Gets table name
   * @return table name
   */
  public static TableName getTableName() {
    return tableName;
  }

  private static void createSystemTableIfNotExists() throws IOException {
    Admin admin = null;
    try {
      admin = connection.getAdmin();
      if (admin.tableExists(tableName) == false) {
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor colDesc = new HColumnDescriptor(familyName);
        colDesc.setMaxVersions(1);
        int ttl =
            config.getInt(HConstants.BACKUP_SYSTEM_TTL_KEY, HConstants.BACKUP_SYSTEM_TTL_DEFAULT);
        colDesc.setTimeToLive(ttl);
        tableDesc.addFamily(colDesc);
        admin.createTable(tableDesc);
      }
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  private BackupSystemTable() {
  }

  /**
   * Updates status (state) of a backup session in hbase:backup table
   * @param context context
   * @throws IOException exception
   */
  public void updateBackupStatus(BackupContext context) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("update backup status in hbase:backup for: " + context.getBackupId()
        + " set status=" + context.getFlag());
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Put put = BackupSystemTableHelper.createPutForBackupContext(context);
      table.put(put);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Deletes backup status from hbase:backup table
   * @param backupId backup id
   * @throws IOException exception
   */

  public void deleteBackupStatus(String backupId) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("delete backup status in hbase:backup for " + backupId);
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Delete del = BackupSystemTableHelper.createDeletForBackupContext(backupId);
      table.delete(del);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Reads backup status object (instance of BackupContext) from hbase:backup table
   * @param backupId - backupId
   * @return Current status of backup session or null
   */

  public BackupContext readBackupStatus(String backupId) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read backup status from hbase:backup for: " + backupId);
    }

    Table table = null;
    try {
      table = connection.getTable(tableName);
      Get get = BackupSystemTableHelper.createGetForBackupContext(backupId);
      Result res = table.get(get);
      if(res.isEmpty()){
        return null;
      }
      return BackupSystemTableHelper.resultToBackupContext(res);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Read the last backup start code (timestamp) of last successful backup. Will return null if
   * there is no start code stored on hbase or the value is of length 0. These two cases indicate
   * there is no successful backup completed so far.
   * @return the timestamp of last successful backup
   * @throws IOException exception
   */
  public String readBackupStartCode() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read backup start code from hbase:backup");
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Get get = BackupSystemTableHelper.createGetForStartCode();
      Result res = table.get(get);
      if (res.isEmpty()){
        return null;
      }
      Cell cell = res.listCells().get(0);
      byte[] val = CellUtil.cloneValue(cell);
      if (val.length == 0){
        return null;
      }
      return new String(val);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Write the start code (timestamp) to hbase:backup. If passed in null, then write 0 byte.
   * @param startCode start code
   * @throws IOException exception
   */
  public void writeBackupStartCode(String startCode) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write backup start code to hbase:backup " + startCode);
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Put put = BackupSystemTableHelper.createPutForStartCode(startCode);
      table.put(put);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Get the Region Servers log information after the last log roll from hbase:backup.
   * @return RS log info
   * @throws IOException exception
   */
  public HashMap<String, String> readRegionServerLastLogRollResult() 
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read region server last roll log result to hbase:backup");
    }
    Table table = null;
    ResultScanner scanner = null;

    try {
      table = connection.getTable(tableName);
      Scan scan = BackupSystemTableHelper.createScanForReadRegionServerLastLogRollResult();
      scan.setMaxVersions(1);
      scanner = table.getScanner(scan);
      Result res = null;
      HashMap<String, String> rsTimestampMap = new HashMap<String, String>();
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String server =
            BackupSystemTableHelper.getServerNameForReadRegionServerLastLogRollResult(row);

        byte[] data = CellUtil.cloneValue(cell);
        rsTimestampMap.put(server, new String(data));
      }
      return rsTimestampMap;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Writes Region Server last roll log result (timestamp) to hbase:backup table
   * @param server - Region Server name
   * @param fileName - last log timestamp
   * @throws IOException exception
   */
  public void writeRegionServerLastLogRollResult(String server, String fileName) 
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write region server last roll log result to hbase:backup");
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Put put = 
          BackupSystemTableHelper.createPutForRegionServerLastLogRollResult(server, fileName);
      table.put(put);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Get all completed backup information (in desc order by time)
   * @return history info of BackupCompleteData
   * @throws IOException exception
   */
  public ArrayList<BackupCompleteData> getBackupHistory() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get backup history from hbase:backup");
    }
    Table table = null;
    ResultScanner scanner = null;
    ArrayList<BackupCompleteData> list = new ArrayList<BackupCompleteData>();
    try {
      table = connection.getTable(tableName);
      Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
      scan.setMaxVersions(1);
      scanner = table.getScanner(scan);
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        BackupContext context = BackupSystemTableHelper.cellToBackupContext(res.current());
        if (context.getFlag() != BACKUPSTATUS.COMPLETE) {
          continue;
        }

        BackupCompleteData history = new BackupCompleteData();
        history.setBackupToken(context.getBackupId());
        history.setStartTime(Long.toString(context.getStartTs()));
        history.setEndTime(Long.toString(context.getEndTs()));
        history.setBackupRootPath(context.getTargetRootDir());
        history.setTableList(context.getTableListAsString());
        history.setType(context.getType());
        history.setBytesCopied(Long.toString(context.getTotalBytesCopied()));

        if (context.fromExistingSnapshot()) {
          history.markFromExistingSnapshot();
        }
        list.add(history);
      }
      return BackupUtil.sortHistoryListDesc(list);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Get all backup session with a given status (in desc order by time)
   * @param status status
   * @return history info of backup contexts
   * @throws IOException exception
   */
  public ArrayList<BackupContext> getBackupContexts(BACKUPSTATUS status) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get backup contexts from hbase:backup");
    }
    Table table = null;
    ResultScanner scanner = null;
    ArrayList<BackupContext> list = new ArrayList<BackupContext>();
    try {
      table = connection.getTable(tableName);
      Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
      scan.setMaxVersions(1);
      scanner = table.getScanner(scan);
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        BackupContext context = BackupSystemTableHelper.cellToBackupContext(res.current());
        if (context.getFlag() != status){
          continue;
        }
        list.add(context);
      }
      return list;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Write the current timestamps for each regionserver to hbase:backup after a successful full or
   * incremental backup. The saved timestamp is of the last log file that was backed up already.
   * @param tables tables
   * @param newTimestamps timestamps
   * @throws IOException exception
   */
  public void writeRegionServerLogTimestamp(Set<String> tables,
      HashMap<String, String> newTimestamps) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write RS log ts to HBASE_BACKUP");
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : newTimestamps.entrySet()) {
      String host = entry.getKey();
      String timestamp = entry.getValue();
      sb.append(host).append(BackupUtil.FIELD_SEPARATOR).append(timestamp)
      .append(BackupUtil.RECORD_SEPARATOR);
    }
    String smap = sb.toString();
    List<Put> puts = new ArrayList<Put>();
    for (String table : tables) {
      Put put = BackupSystemTableHelper.createPutForWriteRegionServerLogTimestamp(table, smap);
      puts.add(put);
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      table.put(puts);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Read the timestamp for each region server log after the last successful backup. Each table has
   * its own set of the timestamps. The info is stored for each table as a concatenated string of
   * rs->timestapmp
   * @return the timestamp for each region server. key: tableName value:
   *         RegionServer,PreviousTimeStamp
   * @throws IOException exception
   */
  public HashMap<String, HashMap<String, String>> readLogTimestampMap() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read RS log ts from HBASE_BACKUP");
    }

    Table table = null;
    ResultScanner scanner = null;
    HashMap<String, HashMap<String, String>> tableTimestampMap =
        new HashMap<String, HashMap<String, String>>();

    try {
      table = connection.getTable(tableName);
      Scan scan = BackupSystemTableHelper.createScanForReadLogTimestampMap();
      scanner = table.getScanner(scan);
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String tabName = BackupSystemTableHelper.getTableNameForReadLogTimestampMap(row);
        HashMap<String, String> lastBackup = new HashMap<String, String>();
        byte[] data = CellUtil.cloneValue(cell);
        if (data == null) {
          // TODO
          throw new IOException("Data of last backup data from HBASE_BACKUP "
              + "is empty. Create a backup first.");
        }
        if (data != null && data.length > 0) {
          String s = new String(data);
          String[] records = s.split(BackupUtil.RECORD_SEPARATOR);
          for (String record : records) {
            String[] flds = record.split(BackupUtil.FIELD_SEPARATOR);
            if (flds.length != 2) {
              throw new IOException("data from HBASE_BACKUP is corrupted: "
                  + Arrays.toString(flds));
            }
            lastBackup.put(flds[0], flds[1]);
          }
          tableTimestampMap.put(tabName, lastBackup);
        }
      }
      return tableTimestampMap;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Return the current tables covered by incremental backup.
   * @return set of tableNames
   * @throws IOException exception
   */
  public Set<String> getIncrementalBackupTableSet() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get incr backup table set from hbase:backup");
    }
    Table table = null;
    TreeSet<String> set = new TreeSet<String>();

    try {
      table = connection.getTable(tableName);
      Get get = BackupSystemTableHelper.createGetForIncrBackupTableSet();
      Result res = table.get(get);
      if (res.isEmpty()) {
        return set;
      }
      List<Cell> cells = res.listCells();
      for (Cell cell : cells) {
        // qualifier = table name - we use table names as qualifiers
        // TODO ns:table as qualifier?
        set.add(new String(CellUtil.cloneQualifier(cell)));
      }
      return set;
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Add tables to global incremental backup set
   * @param tables - set of tables
   * @throws IOException exception
   */
  public void addIncrementalBackupTableSet(Set<String> tables) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("add incr backup table set to hbase:backup");
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Put put = BackupSystemTableHelper.createPutForIncrBackupTableSet(tables);
      table.put(put);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Register WAL files as eligible for deletion
   * @param files files
   * @throws IOException exception
   */
  public void addWALFiles(List<String> files, String backupId) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("add WAL files to hbase:backup");
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      List<Put> puts = BackupSystemTableHelper.createPutsForAddWALFiles(files, backupId);
      table.put(puts);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Check if WAL file is eligible for deletion
   * @param file file
   * @return true, if - yes.
   * @throws IOException exception
   */
  public boolean checkWALFile(String file) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Check if WAL file has been already backuped in hbase:backup");
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Get get = BackupSystemTableHelper.createGetForCheckWALFile(file);
      Result res = table.get(get);
      if (res.isEmpty()){
        return false;
      }
      return true;
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Checks if we have at least one backup session in hbase:backup This API is used by
   * BackupLogCleaner
   * @return true, if - at least one session exists in hbase:backup table
   * @throws IOException exception
   */
  public boolean hasBackupSessions() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("has backup sessions from hbase:backup");
    }
    Table table = null;
    ResultScanner scanner = null;
    boolean result = false;
    try {
      table = connection.getTable(tableName);
      Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
      scan.setMaxVersions(1);
      scan.setCaching(1);
      scanner = table.getScanner(scan);
      if (scanner.next() != null) {
        result = true;
      }
      return result;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
  }
}
