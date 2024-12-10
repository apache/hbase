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
package org.apache.hadoop.hbase.backup.impl;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterators;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * This class provides API to access backup system table<br>
 * Backup system table schema:<br>
 * <p>
 * <ul>
 * <li>1. Backup sessions rowkey= "session:"+backupId; value =serialized BackupInfo</li>
 * <li>2. Backup start code rowkey = "startcode:"+backupRoot; value = startcode</li>
 * <li>3. Incremental backup set rowkey="incrbackupset:"+backupRoot; table="meta:"+tablename of
 * include table; value=empty</li>
 * <li>4. Table-RS-timestamp map rowkey="trslm:"+backupRoot+table_name; value = map[RS-> last WAL
 * timestamp]</li>
 * <li>5. RS - WAL ts map rowkey="rslogts:"+backupRoot +server; value = last WAL timestamp</li>
 * <li>6. WALs recorded rowkey="wals:"+WAL unique file name; value = backupId and full WAL file
 * name</li>
 * </ul>
 * </p>
 */
@InterfaceAudience.Private
public final class BackupSystemTable implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BackupSystemTable.class);

  static class WALItem {
    String backupId;
    String walFile;
    String backupRoot;

    WALItem(String backupId, String walFile, String backupRoot) {
      this.backupId = backupId;
      this.walFile = walFile;
      this.backupRoot = backupRoot;
    }

    public String getBackupId() {
      return backupId;
    }

    public String getWalFile() {
      return walFile;
    }

    public String getBackupRoot() {
      return backupRoot;
    }

    @Override
    public String toString() {
      return Path.SEPARATOR + backupRoot + Path.SEPARATOR + backupId + Path.SEPARATOR + walFile;
    }
  }

  /**
   * Backup system table (main) name
   */
  private TableName tableName;

  /**
   * Backup System table name for bulk loaded files. We keep all bulk loaded file references in a
   * separate table because we have to isolate general backup operations: create, merge etc from
   * activity of RegionObserver, which controls process of a bulk loading
   * {@link org.apache.hadoop.hbase.backup.BackupObserver}
   */
  private TableName bulkLoadTableName;

  /**
   * Stores backup sessions (contexts)
   */
  final static byte[] SESSIONS_FAMILY = Bytes.toBytes("session");
  /**
   * Stores other meta
   */
  final static byte[] META_FAMILY = Bytes.toBytes("meta");
  final static byte[] BULK_LOAD_FAMILY = Bytes.toBytes("bulk");
  /**
   * Connection to HBase cluster, shared among all instances
   */
  private final Connection connection;

  private final static String BACKUP_INFO_PREFIX = "session:";
  private final static String START_CODE_ROW = "startcode:";
  private final static byte[] ACTIVE_SESSION_ROW = Bytes.toBytes("activesession:");
  private final static byte[] ACTIVE_SESSION_COL = Bytes.toBytes("c");

  private final static byte[] ACTIVE_SESSION_YES = Bytes.toBytes("yes");
  private final static byte[] ACTIVE_SESSION_NO = Bytes.toBytes("no");

  private final static String INCR_BACKUP_SET = "incrbackupset:";
  private final static String TABLE_RS_LOG_MAP_PREFIX = "trslm:";
  private final static String RS_LOG_TS_PREFIX = "rslogts:";

  private final static String BULK_LOAD_PREFIX = "bulk:";
  private final static byte[] BULK_LOAD_PREFIX_BYTES = Bytes.toBytes(BULK_LOAD_PREFIX);
  private final static byte[] DELETE_OP_ROW = Bytes.toBytes("delete_op_row");
  private final static byte[] MERGE_OP_ROW = Bytes.toBytes("merge_op_row");

  final static byte[] TBL_COL = Bytes.toBytes("tbl");
  final static byte[] FAM_COL = Bytes.toBytes("fam");
  final static byte[] PATH_COL = Bytes.toBytes("path");

  private final static String SET_KEY_PREFIX = "backupset:";

  // separator between BULK_LOAD_PREFIX and ordinals
  private final static String BLK_LD_DELIM = ":";
  private final static byte[] EMPTY_VALUE = new byte[] {};

  // Safe delimiter in a string
  private final static String NULL = "\u0000";

  public BackupSystemTable(Connection conn) throws IOException {
    this.connection = conn;
    Configuration conf = this.connection.getConfiguration();
    tableName = BackupSystemTable.getTableName(conf);
    bulkLoadTableName = BackupSystemTable.getTableNameForBulkLoadedData(conf);
    checkSystemTable();
  }

  private void checkSystemTable() throws IOException {
    try (Admin admin = connection.getAdmin()) {
      verifyNamespaceExists(admin);
      Configuration conf = connection.getConfiguration();
      if (!admin.tableExists(tableName)) {
        TableDescriptor backupHTD = BackupSystemTable.getSystemTableDescriptor(conf);
        createSystemTable(admin, backupHTD);
      }
      ensureTableEnabled(admin, tableName);
      if (!admin.tableExists(bulkLoadTableName)) {
        TableDescriptor blHTD = BackupSystemTable.getSystemTableForBulkLoadedDataDescriptor(conf);
        createSystemTable(admin, blHTD);
      }
      ensureTableEnabled(admin, bulkLoadTableName);
      waitForSystemTable(admin, tableName);
      waitForSystemTable(admin, bulkLoadTableName);
    }
  }

  private void createSystemTable(Admin admin, TableDescriptor descriptor) throws IOException {
    try {
      admin.createTable(descriptor);
    } catch (TableExistsException e) {
      // swallow because this class is initialized in concurrent environments (i.e. bulkloads),
      // so may be subject to race conditions where one caller succeeds in creating the
      // table and others fail because it now exists
      LOG.debug("Table {} already exists, ignoring", descriptor.getTableName(), e);
    }
  }

  private void verifyNamespaceExists(Admin admin) throws IOException {
    String namespaceName = tableName.getNamespaceAsString();
    NamespaceDescriptor ns = NamespaceDescriptor.create(namespaceName).build();
    NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
    boolean exists = false;
    for (NamespaceDescriptor nsd : list) {
      if (nsd.getName().equals(ns.getName())) {
        exists = true;
        break;
      }
    }
    if (!exists) {
      try {
        admin.createNamespace(ns);
      } catch (NamespaceExistException e) {
        // swallow because this class is initialized in concurrent environments (i.e. bulkloads),
        // so may be subject to race conditions where one caller succeeds in creating the
        // namespace and others fail because it now exists
        LOG.debug("Namespace {} already exists, ignoring", ns.getName(), e);
      }
    }
  }

  private void waitForSystemTable(Admin admin, TableName tableName) throws IOException {
    // Return fast if the table is available and avoid a log message
    if (admin.tableExists(tableName) && admin.isTableAvailable(tableName)) {
      return;
    }
    long TIMEOUT = 60000;
    long startTime = EnvironmentEdgeManager.currentTime();
    LOG.debug("Backup table {} is not present and available, waiting for it to become so",
      tableName);
    while (!admin.tableExists(tableName) || !admin.isTableAvailable(tableName)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException().initCause(e);
      }
      if (EnvironmentEdgeManager.currentTime() - startTime > TIMEOUT) {
        throw new IOException(
          "Failed to create backup system table " + tableName + " after " + TIMEOUT + "ms");
      }
    }
    LOG.debug("Backup table {} exists and available", tableName);
  }

  @Override
  public void close() {
    // do nothing
  }

  /**
   * Updates status (state) of a backup session in backup system table table
   * @param info backup info
   * @throws IOException exception
   */
  public void updateBackupInfo(BackupInfo info) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("update backup status in backup system table for: " + info.getBackupId()
        + " set status=" + info.getState());
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = createPutForBackupInfo(info);
      table.put(put);
    }
  }

  /*
   * @param backupId the backup Id
   * @return Map of rows to path of bulk loaded hfile
   */
  Map<byte[], String> readBulkLoadedFiles(String backupId) throws IOException {
    Scan scan = BackupSystemTable.createScanForBulkLoadedFiles(backupId);
    try (Table table = connection.getTable(bulkLoadTableName);
      ResultScanner scanner = table.getScanner(scan)) {
      Result res = null;
      Map<byte[], String> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      while ((res = scanner.next()) != null) {
        res.advance();
        byte[] row = CellUtil.cloneRow(res.listCells().get(0));
        for (Cell cell : res.listCells()) {
          if (
            CellUtil.compareQualifiers(cell, BackupSystemTable.PATH_COL, 0,
              BackupSystemTable.PATH_COL.length) == 0
          ) {
            map.put(row, Bytes.toString(CellUtil.cloneValue(cell)));
          }
        }
      }
      return map;
    }
  }

  /**
   * Deletes backup status from backup system table table
   * @param backupId backup id
   * @throws IOException exception
   */
  public void deleteBackupInfo(String backupId) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("delete backup status in backup system table for " + backupId);
    }
    try (Table table = connection.getTable(tableName)) {
      Delete del = createDeleteForBackupInfo(backupId);
      table.delete(del);
    }
  }

  /**
   * Registers a bulk load.
   * @param tableName     table name
   * @param region        the region receiving hfile
   * @param cfToHfilePath column family and associated hfiles
   */
  public void registerBulkLoad(TableName tableName, byte[] region,
    Map<byte[], List<Path>> cfToHfilePath) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing bulk load descriptor to backup {} with {} entries", tableName,
        cfToHfilePath.size());
    }
    try (BufferedMutator bufferedMutator = connection.getBufferedMutator(bulkLoadTableName)) {
      List<Put> puts = BackupSystemTable.createPutForBulkLoad(tableName, region, cfToHfilePath);
      bufferedMutator.mutate(puts);
      LOG.debug("Written {} rows for bulk load of {}", puts.size(), tableName);
    }
  }

  /*
   * Removes rows recording bulk loaded hfiles from backup table
   * @param lst list of table names
   * @param rows the rows to be deleted
   */
  public void deleteBulkLoadedRows(List<byte[]> rows) throws IOException {
    try (BufferedMutator bufferedMutator = connection.getBufferedMutator(bulkLoadTableName)) {
      List<Delete> lstDels = new ArrayList<>();
      for (byte[] row : rows) {
        Delete del = new Delete(row);
        lstDels.add(del);
        LOG.debug("orig deleting the row: " + Bytes.toString(row));
      }
      bufferedMutator.mutate(lstDels);
      LOG.debug("deleted " + rows.size() + " original bulkload rows");
    }
  }

  /**
   * Reads all registered bulk loads.
   */
  public List<BulkLoad> readBulkloadRows() throws IOException {
    Scan scan = BackupSystemTable.createScanForOrigBulkLoadedFiles(null);
    return processBulkLoadRowScan(scan);
  }

  /**
   * Reads the registered bulk loads for the given tables.
   */
  public List<BulkLoad> readBulkloadRows(Collection<TableName> tableList) throws IOException {
    List<BulkLoad> result = new ArrayList<>();
    for (TableName table : tableList) {
      Scan scan = BackupSystemTable.createScanForOrigBulkLoadedFiles(table);
      result.addAll(processBulkLoadRowScan(scan));
    }
    return result;
  }

  private List<BulkLoad> processBulkLoadRowScan(Scan scan) throws IOException {
    List<BulkLoad> result = new ArrayList<>();
    try (Table bulkLoadTable = connection.getTable(bulkLoadTableName);
      ResultScanner scanner = bulkLoadTable.getScanner(scan)) {
      Result res;
      while ((res = scanner.next()) != null) {
        res.advance();
        TableName table = null;
        String fam = null;
        String path = null;
        String region = null;
        byte[] row = null;
        for (Cell cell : res.listCells()) {
          row = CellUtil.cloneRow(cell);
          String rowStr = Bytes.toString(row);
          region = BackupSystemTable.getRegionNameFromOrigBulkLoadRow(rowStr);
          if (
            CellUtil.compareQualifiers(cell, BackupSystemTable.TBL_COL, 0,
              BackupSystemTable.TBL_COL.length) == 0
          ) {
            table = TableName.valueOf(CellUtil.cloneValue(cell));
          } else if (
            CellUtil.compareQualifiers(cell, BackupSystemTable.FAM_COL, 0,
              BackupSystemTable.FAM_COL.length) == 0
          ) {
            fam = Bytes.toString(CellUtil.cloneValue(cell));
          } else if (
            CellUtil.compareQualifiers(cell, BackupSystemTable.PATH_COL, 0,
              BackupSystemTable.PATH_COL.length) == 0
          ) {
            path = Bytes.toString(CellUtil.cloneValue(cell));
          }
        }
        result.add(new BulkLoad(table, region, fam, path, row));
        LOG.debug("Found bulk load entry for table {}, family {}: {}", table, fam, path);
      }
    }
    return result;
  }

  /**
   * Reads backup status object (instance of backup info) from backup system table table
   * @param backupId backup id
   * @return Current status of backup session or null
   */
  public BackupInfo readBackupInfo(String backupId) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("read backup status from backup system table for: " + backupId);
    }

    try (Table table = connection.getTable(tableName)) {
      Get get = createGetForBackupInfo(backupId);
      Result res = table.get(get);
      if (res.isEmpty()) {
        return null;
      }
      return resultToBackupInfo(res);
    }
  }

  /**
   * Read the last backup start code (timestamp) of last successful backup. Will return null if
   * there is no start code stored on hbase or the value is of length 0. These two cases indicate
   * there is no successful backup completed so far.
   * @param backupRoot directory path to backup destination
   * @return the timestamp of last successful backup
   * @throws IOException exception
   */
  public String readBackupStartCode(String backupRoot) throws IOException {
    LOG.trace("read backup start code from backup system table");

    try (Table table = connection.getTable(tableName)) {
      Get get = createGetForStartCode(backupRoot);
      Result res = table.get(get);
      if (res.isEmpty()) {
        return null;
      }
      Cell cell = res.listCells().get(0);
      byte[] val = CellUtil.cloneValue(cell);
      if (val.length == 0) {
        return null;
      }
      return new String(val, StandardCharsets.UTF_8);
    }
  }

  /**
   * Write the start code (timestamp) to backup system table. If passed in null, then write 0 byte.
   * @param startCode  start code
   * @param backupRoot root directory path to backup
   * @throws IOException exception
   */
  public void writeBackupStartCode(Long startCode, String backupRoot) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("write backup start code to backup system table " + startCode);
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = createPutForStartCode(startCode.toString(), backupRoot);
      table.put(put);
    }
  }

  /**
   * Exclusive operations are: create, delete, merge
   * @throws IOException if a table operation fails or an active backup exclusive operation is
   *                     already underway
   */
  public void startBackupExclusiveOperation() throws IOException {
    LOG.debug("Start new backup exclusive operation");

    try (Table table = connection.getTable(tableName)) {
      Put put = createPutForStartBackupSession();
      // First try to put if row does not exist
      if (
        !table.checkAndMutate(ACTIVE_SESSION_ROW, SESSIONS_FAMILY).qualifier(ACTIVE_SESSION_COL)
          .ifNotExists().thenPut(put)
      ) {
        // Row exists, try to put if value == ACTIVE_SESSION_NO
        if (
          !table.checkAndMutate(ACTIVE_SESSION_ROW, SESSIONS_FAMILY).qualifier(ACTIVE_SESSION_COL)
            .ifEquals(ACTIVE_SESSION_NO).thenPut(put)
        ) {
          throw new ExclusiveOperationException();
        }
      }
    }
  }

  private Put createPutForStartBackupSession() {
    Put put = new Put(ACTIVE_SESSION_ROW);
    put.addColumn(SESSIONS_FAMILY, ACTIVE_SESSION_COL, ACTIVE_SESSION_YES);
    return put;
  }

  public void finishBackupExclusiveOperation() throws IOException {
    LOG.debug("Finish backup exclusive operation");

    try (Table table = connection.getTable(tableName)) {
      Put put = createPutForStopBackupSession();
      if (
        !table.checkAndMutate(ACTIVE_SESSION_ROW, SESSIONS_FAMILY).qualifier(ACTIVE_SESSION_COL)
          .ifEquals(ACTIVE_SESSION_YES).thenPut(put)
      ) {
        throw new IOException("There is no active backup exclusive operation");
      }
    }
  }

  private Put createPutForStopBackupSession() {
    Put put = new Put(ACTIVE_SESSION_ROW);
    put.addColumn(SESSIONS_FAMILY, ACTIVE_SESSION_COL, ACTIVE_SESSION_NO);
    return put;
  }

  /**
   * Get the Region Servers log information after the last log roll from backup system table.
   * @param backupRoot root directory path to backup
   * @return RS log info
   * @throws IOException exception
   */
  public HashMap<String, Long> readRegionServerLastLogRollResult(String backupRoot)
    throws IOException {
    LOG.trace("read region server last roll log result to backup system table");

    Scan scan = createScanForReadRegionServerLastLogRollResult(backupRoot);

    try (Table table = connection.getTable(tableName);
      ResultScanner scanner = table.getScanner(scan)) {
      Result res;
      HashMap<String, Long> rsTimestampMap = new HashMap<>();
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String server = getServerNameForReadRegionServerLastLogRollResult(row);
        byte[] data = CellUtil.cloneValue(cell);
        rsTimestampMap.put(server, Bytes.toLong(data));
      }
      return rsTimestampMap;
    }
  }

  /**
   * Writes Region Server last roll log result (timestamp) to backup system table table
   * @param server     Region Server name
   * @param ts         last log timestamp
   * @param backupRoot root directory path to backup
   * @throws IOException exception
   */
  public void writeRegionServerLastLogRollResult(String server, Long ts, String backupRoot)
    throws IOException {
    LOG.trace("write region server last roll log result to backup system table");

    try (Table table = connection.getTable(tableName)) {
      Put put = createPutForRegionServerLastLogRollResult(server, ts, backupRoot);
      table.put(put);
    }
  }

  /**
   * Get all completed backup information (in desc order by time)
   * @param onlyCompleted true, if only successfully completed sessions
   * @return history info of BackupCompleteData
   * @throws IOException exception
   */
  public ArrayList<BackupInfo> getBackupHistory(boolean onlyCompleted) throws IOException {
    LOG.trace("get backup history from backup system table");

    BackupState state = onlyCompleted ? BackupState.COMPLETE : BackupState.ANY;
    ArrayList<BackupInfo> list = getBackupInfos(state);
    return BackupUtils.sortHistoryListDesc(list);
  }

  /**
   * Get all backups history
   * @return list of backup info
   * @throws IOException if getting the backup history fails
   */
  public List<BackupInfo> getBackupHistory() throws IOException {
    return getBackupHistory(false);
  }

  /**
   * Get first n backup history records
   * @param n number of records, if n== -1 - max number is ignored
   * @return list of records
   * @throws IOException if getting the backup history fails
   */
  public List<BackupInfo> getHistory(int n) throws IOException {
    List<BackupInfo> history = getBackupHistory();
    if (n == -1 || history.size() <= n) {
      return history;
    }
    return Collections.unmodifiableList(history.subList(0, n));
  }

  /**
   * Get backup history records filtered by list of filters.
   * @param n       max number of records, if n == -1 , then max number is ignored
   * @param filters list of filters
   * @return backup records
   * @throws IOException if getting the backup history fails
   */
  public List<BackupInfo> getBackupHistory(int n, BackupInfo.Filter... filters) throws IOException {
    if (filters.length == 0) {
      return getHistory(n);
    }

    List<BackupInfo> history = getBackupHistory();
    List<BackupInfo> result = new ArrayList<>();
    for (BackupInfo bi : history) {
      if (n >= 0 && result.size() == n) {
        break;
      }

      boolean passed = true;
      for (int i = 0; i < filters.length; i++) {
        if (!filters[i].apply(bi)) {
          passed = false;
          break;
        }
      }
      if (passed) {
        result.add(bi);
      }
    }
    return result;
  }

  /**
   * Retrieve all table names that are part of any known backup
   */
  public Set<TableName> getTablesIncludedInBackups() throws IOException {
    Set<TableName> names = new HashSet<>();
    List<BackupInfo> infos = getBackupHistory(true);
    for (BackupInfo info : infos) {
      // Incremental backups have the same tables as the preceding full backups
      if (info.getType() == BackupType.FULL) {
        names.addAll(info.getTableNames());
      }
    }
    return names;
  }

  /**
   * Get history for backup destination
   * @param backupRoot backup destination path
   * @return List of backup info
   * @throws IOException if getting the backup history fails
   */
  public List<BackupInfo> getBackupHistory(String backupRoot) throws IOException {
    ArrayList<BackupInfo> history = getBackupHistory(false);
    for (Iterator<BackupInfo> iterator = history.iterator(); iterator.hasNext();) {
      BackupInfo info = iterator.next();
      if (!backupRoot.equals(info.getBackupRootDir())) {
        iterator.remove();
      }
    }
    return history;
  }

  /**
   * Get history for a table
   * @param name table name
   * @return history for a table
   * @throws IOException if getting the backup history fails
   */
  public List<BackupInfo> getBackupHistoryForTable(TableName name) throws IOException {
    List<BackupInfo> history = getBackupHistory();
    List<BackupInfo> tableHistory = new ArrayList<>();
    for (BackupInfo info : history) {
      List<TableName> tables = info.getTableNames();
      if (tables.contains(name)) {
        tableHistory.add(info);
      }
    }
    return tableHistory;
  }

  /**
   * Goes through all backup history corresponding to the provided root folder, and collects all
   * backup info mentioning each of the provided tables.
   * @param set        the tables for which to collect the {@code BackupInfo}
   * @param backupRoot backup destination path to retrieve backup history for
   * @return a map containing (a subset of) the provided {@code TableName}s, mapped to a list of at
   *         least one {@code BackupInfo}
   * @throws IOException if getting the backup history fails
   */
  public Map<TableName, List<BackupInfo>> getBackupHistoryForTableSet(Set<TableName> set,
    String backupRoot) throws IOException {
    List<BackupInfo> history = getBackupHistory(backupRoot);
    Map<TableName, List<BackupInfo>> tableHistoryMap = new HashMap<>();
    for (BackupInfo info : history) {
      List<TableName> tables = info.getTableNames();
      for (TableName tableName : tables) {
        if (set.contains(tableName)) {
          List<BackupInfo> list =
            tableHistoryMap.computeIfAbsent(tableName, k -> new ArrayList<>());
          list.add(info);
        }
      }
    }
    return tableHistoryMap;
  }

  /**
   * Get all backup sessions with a given state (in descending order by time)
   * @param state backup session state
   * @return history info of backup info objects
   * @throws IOException exception
   */
  public ArrayList<BackupInfo> getBackupInfos(BackupState state) throws IOException {
    LOG.trace("get backup infos from backup system table");

    Scan scan = createScanForBackupHistory();
    ArrayList<BackupInfo> list = new ArrayList<>();

    try (Table table = connection.getTable(tableName);
      ResultScanner scanner = table.getScanner(scan)) {
      Result res;
      while ((res = scanner.next()) != null) {
        res.advance();
        BackupInfo context = cellToBackupInfo(res.current());
        if (state != BackupState.ANY && context.getState() != state) {
          continue;
        }
        list.add(context);
      }
      return list;
    }
  }

  /**
   * Write the current timestamps for each regionserver to backup system table after a successful
   * full or incremental backup. The saved timestamp is of the last log file that was backed up
   * already.
   * @param tables        tables
   * @param newTimestamps timestamps
   * @param backupRoot    root directory path to backup
   * @throws IOException exception
   */
  public void writeRegionServerLogTimestamp(Set<TableName> tables, Map<String, Long> newTimestamps,
    String backupRoot) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("write RS log time stamps to backup system table for tables ["
        + StringUtils.join(tables, ",") + "]");
    }
    List<Put> puts = new ArrayList<>();
    for (TableName table : tables) {
      byte[] smapData = toTableServerTimestampProto(table, newTimestamps).toByteArray();
      Put put = createPutForWriteRegionServerLogTimestamp(table, smapData, backupRoot);
      puts.add(put);
    }
    try (BufferedMutator bufferedMutator = connection.getBufferedMutator(tableName)) {
      bufferedMutator.mutate(puts);
    }
  }

  /**
   * Read the timestamp for each region server log after the last successful backup. Each table has
   * its own set of the timestamps. The info is stored for each table as a concatenated string of
   * rs->timestapmp
   * @param backupRoot root directory path to backup
   * @return the timestamp for each region server. key: tableName value:
   *         RegionServer,PreviousTimeStamp
   * @throws IOException exception
   */
  public Map<TableName, Map<String, Long>> readLogTimestampMap(String backupRoot)
    throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("read RS log ts from backup system table for root=" + backupRoot);
    }

    Map<TableName, Map<String, Long>> tableTimestampMap = new HashMap<>();

    Scan scan = createScanForReadLogTimestampMap(backupRoot);
    try (Table table = connection.getTable(tableName);
      ResultScanner scanner = table.getScanner(scan)) {
      Result res;
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String tabName = getTableNameForReadLogTimestampMap(row);
        TableName tn = TableName.valueOf(tabName);
        byte[] data = CellUtil.cloneValue(cell);
        if (data == null) {
          throw new IOException("Data of last backup data from backup system table "
            + "is empty. Create a backup first.");
        }
        if (data != null && data.length > 0) {
          HashMap<String, Long> lastBackup =
            fromTableServerTimestampProto(BackupProtos.TableServerTimestamp.parseFrom(data));
          tableTimestampMap.put(tn, lastBackup);
        }
      }
      return tableTimestampMap;
    }
  }

  private BackupProtos.TableServerTimestamp toTableServerTimestampProto(TableName table,
    Map<String, Long> map) {
    BackupProtos.TableServerTimestamp.Builder tstBuilder =
      BackupProtos.TableServerTimestamp.newBuilder();
    tstBuilder
      .setTableName(org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toProtoTableName(table));

    for (Entry<String, Long> entry : map.entrySet()) {
      BackupProtos.ServerTimestamp.Builder builder = BackupProtos.ServerTimestamp.newBuilder();
      HBaseProtos.ServerName.Builder snBuilder = HBaseProtos.ServerName.newBuilder();
      ServerName sn = ServerName.parseServerName(entry.getKey());
      snBuilder.setHostName(sn.getHostname());
      snBuilder.setPort(sn.getPort());
      builder.setServerName(snBuilder.build());
      builder.setTimestamp(entry.getValue());
      tstBuilder.addServerTimestamp(builder.build());
    }

    return tstBuilder.build();
  }

  private HashMap<String, Long>
    fromTableServerTimestampProto(BackupProtos.TableServerTimestamp proto) {

    HashMap<String, Long> map = new HashMap<>();
    List<BackupProtos.ServerTimestamp> list = proto.getServerTimestampList();
    for (BackupProtos.ServerTimestamp st : list) {
      ServerName sn =
        org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toServerName(st.getServerName());
      map.put(sn.getHostname() + ":" + sn.getPort(), st.getTimestamp());
    }
    return map;
  }

  /**
   * Return the current tables covered by incremental backup.
   * @param backupRoot root directory path to backup
   * @return set of tableNames
   * @throws IOException exception
   */
  public Set<TableName> getIncrementalBackupTableSet(String backupRoot) throws IOException {
    LOG.trace("get incremental backup table set from backup system table");

    TreeSet<TableName> set = new TreeSet<>();

    try (Table table = connection.getTable(tableName)) {
      Get get = createGetForIncrBackupTableSet(backupRoot);
      Result res = table.get(get);
      if (res.isEmpty()) {
        return set;
      }
      List<Cell> cells = res.listCells();
      for (Cell cell : cells) {
        // qualifier = table name - we use table names as qualifiers
        set.add(TableName.valueOf(CellUtil.cloneQualifier(cell)));
      }
      return set;
    }
  }

  /**
   * Add tables to global incremental backup set
   * @param tables     set of tables
   * @param backupRoot root directory path to backup
   * @throws IOException exception
   */
  public void addIncrementalBackupTableSet(Set<TableName> tables, String backupRoot)
    throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Add incremental backup table set to backup system table. ROOT=" + backupRoot
        + " tables [" + StringUtils.join(tables, " ") + "]");
    }
    if (LOG.isDebugEnabled()) {
      tables.forEach(table -> LOG.debug(Objects.toString(table)));
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = createPutForIncrBackupTableSet(tables, backupRoot);
      table.put(put);
    }
  }

  /**
   * Deletes incremental backup set for a backup destination
   * @param backupRoot backup root
   */
  public void deleteIncrementalBackupTableSet(String backupRoot) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Delete incremental backup table set to backup system table. ROOT=" + backupRoot);
    }
    try (Table table = connection.getTable(tableName)) {
      Delete delete = createDeleteForIncrBackupTableSet(backupRoot);
      table.delete(delete);
    }
  }

  /**
   * Checks if we have at least one backup session in backup system table This API is used by
   * BackupLogCleaner
   * @return true, if - at least one session exists in backup system table table
   * @throws IOException exception
   */
  public boolean hasBackupSessions() throws IOException {
    LOG.trace("Has backup sessions from backup system table");

    boolean result = false;
    Scan scan = createScanForBackupHistory();
    scan.setCaching(1);
    try (Table table = connection.getTable(tableName);
      ResultScanner scanner = table.getScanner(scan)) {
      if (scanner.next() != null) {
        result = true;
      }
      return result;
    }
  }

  /**
   * BACKUP SETS
   */

  /**
   * Get backup set list
   * @return backup set list
   * @throws IOException if a table or scanner operation fails
   */
  public List<String> listBackupSets() throws IOException {
    LOG.trace("Backup set list");

    List<String> list = new ArrayList<>();
    try (Table table = connection.getTable(tableName)) {
      Scan scan = createScanForBackupSetList();
      scan.readVersions(1);
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result res;
        while ((res = scanner.next()) != null) {
          res.advance();
          list.add(cellKeyToBackupSetName(res.current()));
        }
        return list;
      }
    }
  }

  /**
   * Get backup set description (list of tables)
   * @param name set's name
   * @return list of tables in a backup set
   * @throws IOException if a table operation fails
   */
  public List<TableName> describeBackupSet(String name) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(" Backup set describe: " + name);
    }
    try (Table table = connection.getTable(tableName)) {
      Get get = createGetForBackupSet(name);
      Result res = table.get(get);
      if (res.isEmpty()) {
        return null;
      }
      res.advance();
      String[] tables = cellValueToBackupSet(res.current());
      return Arrays.asList(tables).stream().map(item -> TableName.valueOf(item))
        .collect(Collectors.toList());
    }
  }

  /**
   * Add backup set (list of tables)
   * @param name      set name
   * @param newTables list of tables, comma-separated
   * @throws IOException if a table operation fails
   */
  public void addToBackupSet(String name, String[] newTables) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Backup set add: " + name + " tables [" + StringUtils.join(newTables, " ") + "]");
    }
    String[] union = null;
    try (Table table = connection.getTable(tableName)) {
      Get get = createGetForBackupSet(name);
      Result res = table.get(get);
      if (res.isEmpty()) {
        union = newTables;
      } else {
        res.advance();
        String[] tables = cellValueToBackupSet(res.current());
        union = merge(tables, newTables);
      }
      Put put = createPutForBackupSet(name, union);
      table.put(put);
    }
  }

  /**
   * Remove tables from backup set (list of tables)
   * @param name     set name
   * @param toRemove list of tables
   * @throws IOException if a table operation or deleting the backup set fails
   */
  public void removeFromBackupSet(String name, String[] toRemove) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        " Backup set remove from : " + name + " tables [" + StringUtils.join(toRemove, " ") + "]");
    }
    String[] disjoint;
    String[] tables;
    try (Table table = connection.getTable(tableName)) {
      Get get = createGetForBackupSet(name);
      Result res = table.get(get);
      if (res.isEmpty()) {
        LOG.warn("Backup set '" + name + "' not found.");
        return;
      } else {
        res.advance();
        tables = cellValueToBackupSet(res.current());
        disjoint = disjoin(tables, toRemove);
      }
      if (disjoint.length > 0 && disjoint.length != tables.length) {
        Put put = createPutForBackupSet(name, disjoint);
        table.put(put);
      } else if (disjoint.length == tables.length) {
        LOG.warn("Backup set '" + name + "' does not contain tables ["
          + StringUtils.join(toRemove, " ") + "]");
      } else { // disjoint.length == 0 and tables.length >0
        // Delete backup set
        LOG.info("Backup set '" + name + "' is empty. Deleting.");
        deleteBackupSet(name);
      }
    }
  }

  private String[] merge(String[] existingTables, String[] newTables) {
    Set<String> tables = new HashSet<>(Arrays.asList(existingTables));
    tables.addAll(Arrays.asList(newTables));
    return tables.toArray(new String[0]);
  }

  private String[] disjoin(String[] existingTables, String[] toRemove) {
    Set<String> tables = new HashSet<>(Arrays.asList(existingTables));
    Arrays.asList(toRemove).forEach(table -> tables.remove(table));
    return tables.toArray(new String[0]);
  }

  /**
   * Delete backup set
   * @param name set's name
   * @throws IOException if getting or deleting the table fails
   */
  public void deleteBackupSet(String name) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(" Backup set delete: " + name);
    }
    try (Table table = connection.getTable(tableName)) {
      Delete del = createDeleteForBackupSet(name);
      table.delete(del);
    }
  }

  /**
   * Get backup system table descriptor
   * @return table's descriptor
   */
  public static TableDescriptor getSystemTableDescriptor(Configuration conf) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(getTableName(conf));

    ColumnFamilyDescriptorBuilder colBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(SESSIONS_FAMILY);

    colBuilder.setMaxVersions(1);
    Configuration config = HBaseConfiguration.create();
    int ttl = config.getInt(BackupRestoreConstants.BACKUP_SYSTEM_TTL_KEY,
      BackupRestoreConstants.BACKUP_SYSTEM_TTL_DEFAULT);
    colBuilder.setTimeToLive(ttl);

    ColumnFamilyDescriptor colSessionsDesc = colBuilder.build();
    builder.setColumnFamily(colSessionsDesc);

    colBuilder = ColumnFamilyDescriptorBuilder.newBuilder(META_FAMILY);
    colBuilder.setTimeToLive(ttl);
    builder.setColumnFamily(colBuilder.build());
    return builder.build();
  }

  public static TableName getTableName(Configuration conf) {
    String name = conf.get(BackupRestoreConstants.BACKUP_SYSTEM_TABLE_NAME_KEY,
      BackupRestoreConstants.BACKUP_SYSTEM_TABLE_NAME_DEFAULT);
    return TableName.valueOf(name);
  }

  public static String getTableNameAsString(Configuration conf) {
    return getTableName(conf).getNameAsString();
  }

  public static String getSnapshotName(Configuration conf) {
    return "snapshot_" + getTableNameAsString(conf).replace(":", "_");
  }

  /**
   * Get backup system table descriptor
   * @return table's descriptor
   */
  public static TableDescriptor getSystemTableForBulkLoadedDataDescriptor(Configuration conf) {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(getTableNameForBulkLoadedData(conf));

    ColumnFamilyDescriptorBuilder colBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(SESSIONS_FAMILY);
    colBuilder.setMaxVersions(1);
    Configuration config = HBaseConfiguration.create();
    int ttl = config.getInt(BackupRestoreConstants.BACKUP_SYSTEM_TTL_KEY,
      BackupRestoreConstants.BACKUP_SYSTEM_TTL_DEFAULT);
    colBuilder.setTimeToLive(ttl);
    ColumnFamilyDescriptor colSessionsDesc = colBuilder.build();
    builder.setColumnFamily(colSessionsDesc);
    colBuilder = ColumnFamilyDescriptorBuilder.newBuilder(META_FAMILY);
    colBuilder.setTimeToLive(ttl);
    builder.setColumnFamily(colBuilder.build());
    return builder.build();
  }

  public static TableName getTableNameForBulkLoadedData(Configuration conf) {
    String name = conf.get(BackupRestoreConstants.BACKUP_SYSTEM_TABLE_NAME_KEY,
      BackupRestoreConstants.BACKUP_SYSTEM_TABLE_NAME_DEFAULT) + "_bulk";
    return TableName.valueOf(name);
  }

  /**
   * Creates Put operation for a given backup info object
   * @param context backup info
   * @return put operation
   * @throws IOException exception
   */
  private Put createPutForBackupInfo(BackupInfo context) throws IOException {
    Put put = new Put(rowkey(BACKUP_INFO_PREFIX, context.getBackupId()));
    put.addColumn(BackupSystemTable.SESSIONS_FAMILY, Bytes.toBytes("context"),
      context.toByteArray());
    return put;
  }

  /**
   * Creates Get operation for a given backup id
   * @param backupId backup's ID
   * @return get operation
   * @throws IOException exception
   */
  private Get createGetForBackupInfo(String backupId) throws IOException {
    Get get = new Get(rowkey(BACKUP_INFO_PREFIX, backupId));
    get.addFamily(BackupSystemTable.SESSIONS_FAMILY);
    get.readVersions(1);
    return get;
  }

  /**
   * Creates Delete operation for a given backup id
   * @param backupId backup's ID
   * @return delete operation
   */
  private Delete createDeleteForBackupInfo(String backupId) {
    Delete del = new Delete(rowkey(BACKUP_INFO_PREFIX, backupId));
    del.addFamily(BackupSystemTable.SESSIONS_FAMILY);
    return del;
  }

  /**
   * Converts Result to BackupInfo
   * @param res HBase result
   * @return backup info instance
   * @throws IOException exception
   */
  private BackupInfo resultToBackupInfo(Result res) throws IOException {
    res.advance();
    Cell cell = res.current();
    return cellToBackupInfo(cell);
  }

  /**
   * Creates Get operation to retrieve start code from backup system table
   * @return get operation
   * @throws IOException exception
   */
  private Get createGetForStartCode(String rootPath) throws IOException {
    Get get = new Get(rowkey(START_CODE_ROW, rootPath));
    get.addFamily(BackupSystemTable.META_FAMILY);
    get.readVersions(1);
    return get;
  }

  /**
   * Creates Put operation to store start code to backup system table
   * @return put operation
   */
  private Put createPutForStartCode(String startCode, String rootPath) {
    Put put = new Put(rowkey(START_CODE_ROW, rootPath));
    put.addColumn(BackupSystemTable.META_FAMILY, Bytes.toBytes("startcode"),
      Bytes.toBytes(startCode));
    return put;
  }

  /**
   * Creates Get to retrieve incremental backup table set from backup system table
   * @return get operation
   * @throws IOException exception
   */
  private Get createGetForIncrBackupTableSet(String backupRoot) throws IOException {
    Get get = new Get(rowkey(INCR_BACKUP_SET, backupRoot));
    get.addFamily(BackupSystemTable.META_FAMILY);
    get.readVersions(1);
    return get;
  }

  /**
   * Creates Put to store incremental backup table set
   * @param tables tables
   * @return put operation
   */
  private Put createPutForIncrBackupTableSet(Set<TableName> tables, String backupRoot) {
    Put put = new Put(rowkey(INCR_BACKUP_SET, backupRoot));
    for (TableName table : tables) {
      put.addColumn(BackupSystemTable.META_FAMILY, Bytes.toBytes(table.getNameAsString()),
        EMPTY_VALUE);
    }
    return put;
  }

  /**
   * Creates Delete for incremental backup table set
   * @param backupRoot backup root
   * @return delete operation
   */
  private Delete createDeleteForIncrBackupTableSet(String backupRoot) {
    Delete delete = new Delete(rowkey(INCR_BACKUP_SET, backupRoot));
    delete.addFamily(BackupSystemTable.META_FAMILY);
    return delete;
  }

  /**
   * Creates Scan operation to load backup history
   * @return scan operation
   */
  private Scan createScanForBackupHistory() {
    Scan scan = new Scan();
    byte[] startRow = Bytes.toBytes(BACKUP_INFO_PREFIX);
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.withStartRow(startRow);
    scan.withStopRow(stopRow);
    scan.addFamily(BackupSystemTable.SESSIONS_FAMILY);
    scan.readVersions(1);
    return scan;
  }

  /**
   * Converts cell to backup info instance.
   * @param current current cell
   * @return backup backup info instance
   * @throws IOException exception
   */
  private BackupInfo cellToBackupInfo(Cell current) throws IOException {
    byte[] data = CellUtil.cloneValue(current);
    return BackupInfo.fromByteArray(data);
  }

  /**
   * Creates Put to write RS last roll log timestamp map
   * @param table table
   * @param smap  map, containing RS:ts
   * @return put operation
   */
  private Put createPutForWriteRegionServerLogTimestamp(TableName table, byte[] smap,
    String backupRoot) {
    Put put = new Put(rowkey(TABLE_RS_LOG_MAP_PREFIX, backupRoot, NULL, table.getNameAsString()));
    put.addColumn(BackupSystemTable.META_FAMILY, Bytes.toBytes("log-roll-map"), smap);
    return put;
  }

  /**
   * Creates Scan to load table-> { RS -> ts} map of maps
   * @return scan operation
   */
  private Scan createScanForReadLogTimestampMap(String backupRoot) {
    Scan scan = new Scan();
    scan.setStartStopRowForPrefixScan(rowkey(TABLE_RS_LOG_MAP_PREFIX, backupRoot, NULL));
    scan.addFamily(BackupSystemTable.META_FAMILY);

    return scan;
  }

  /**
   * Get table name from rowkey
   * @param cloneRow rowkey
   * @return table name
   */
  private String getTableNameForReadLogTimestampMap(byte[] cloneRow) {
    String s = Bytes.toString(cloneRow);
    int index = s.lastIndexOf(NULL);
    return s.substring(index + 1);
  }

  /**
   * Creates Put to store RS last log result
   * @param server    server name
   * @param timestamp log roll result (timestamp)
   * @return put operation
   */
  private Put createPutForRegionServerLastLogRollResult(String server, Long timestamp,
    String backupRoot) {
    Put put = new Put(rowkey(RS_LOG_TS_PREFIX, backupRoot, NULL, server));
    put.addColumn(BackupSystemTable.META_FAMILY, Bytes.toBytes("rs-log-ts"),
      Bytes.toBytes(timestamp));
    return put;
  }

  /**
   * Creates Scan operation to load last RS log roll results
   * @return scan operation
   */
  private Scan createScanForReadRegionServerLastLogRollResult(String backupRoot) {
    Scan scan = new Scan();
    scan.setStartStopRowForPrefixScan(rowkey(RS_LOG_TS_PREFIX, backupRoot, NULL));
    scan.addFamily(BackupSystemTable.META_FAMILY);
    scan.readVersions(1);

    return scan;
  }

  /**
   * Get server's name from rowkey
   * @param row rowkey
   * @return server's name
   */
  private String getServerNameForReadRegionServerLastLogRollResult(byte[] row) {
    String s = Bytes.toString(row);
    int index = s.lastIndexOf(NULL);
    return s.substring(index + 1);
  }

  /**
   * Creates Put's for bulk loads.
   */
  private static List<Put> createPutForBulkLoad(TableName table, byte[] region,
    Map<byte[], List<Path>> columnFamilyToHFilePaths) {
    List<Put> puts = new ArrayList<>();
    for (Map.Entry<byte[], List<Path>> entry : columnFamilyToHFilePaths.entrySet()) {
      for (Path path : entry.getValue()) {
        String file = path.toString();
        int lastSlash = file.lastIndexOf("/");
        String filename = file.substring(lastSlash + 1);
        Put put = new Put(rowkey(BULK_LOAD_PREFIX, table.toString(), BLK_LD_DELIM,
          Bytes.toString(region), BLK_LD_DELIM, filename));
        put.addColumn(BackupSystemTable.META_FAMILY, TBL_COL, table.getName());
        put.addColumn(BackupSystemTable.META_FAMILY, FAM_COL, entry.getKey());
        put.addColumn(BackupSystemTable.META_FAMILY, PATH_COL, Bytes.toBytes(file));
        puts.add(put);
        LOG.debug("Done writing bulk path {} for {} {}", file, table, Bytes.toString(region));
      }
    }
    return puts;
  }

  public static void snapshot(Connection conn) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      Configuration conf = conn.getConfiguration();
      admin.snapshot(BackupSystemTable.getSnapshotName(conf), BackupSystemTable.getTableName(conf));
    }
  }

  public static void restoreFromSnapshot(Connection conn) throws IOException {
    Configuration conf = conn.getConfiguration();
    LOG.debug("Restoring " + BackupSystemTable.getTableNameAsString(conf) + " from snapshot");
    try (Admin admin = conn.getAdmin()) {
      String snapshotName = BackupSystemTable.getSnapshotName(conf);
      if (snapshotExists(admin, snapshotName)) {
        admin.disableTable(BackupSystemTable.getTableName(conf));
        admin.restoreSnapshot(snapshotName);
        admin.enableTable(BackupSystemTable.getTableName(conf));
        LOG.debug("Done restoring backup system table");
      } else {
        // Snapshot does not exists, i.e completeBackup failed after
        // deleting backup system table snapshot
        // In this case we log WARN and proceed
        LOG.warn(
          "Could not restore backup system table. Snapshot " + snapshotName + " does not exists.");
      }
    }
  }

  private static boolean snapshotExists(Admin admin, String snapshotName) throws IOException {
    List<SnapshotDescription> list = admin.listSnapshots();
    for (SnapshotDescription desc : list) {
      if (desc.getName().equals(snapshotName)) {
        return true;
      }
    }
    return false;
  }

  public static boolean snapshotExists(Connection conn) throws IOException {
    return snapshotExists(conn.getAdmin(), getSnapshotName(conn.getConfiguration()));
  }

  public static void deleteSnapshot(Connection conn) throws IOException {
    Configuration conf = conn.getConfiguration();
    LOG.debug("Deleting " + BackupSystemTable.getSnapshotName(conf) + " from the system");
    try (Admin admin = conn.getAdmin()) {
      String snapshotName = BackupSystemTable.getSnapshotName(conf);
      if (snapshotExists(admin, snapshotName)) {
        admin.deleteSnapshot(snapshotName);
        LOG.debug("Done deleting backup system table snapshot");
      } else {
        LOG.error("Snapshot " + snapshotName + " does not exists");
      }
    }
  }

  public static List<Delete> createDeleteForOrigBulkLoad(List<TableName> lst) {
    List<Delete> lstDels = new ArrayList<>(lst.size());
    for (TableName table : lst) {
      Delete del = new Delete(rowkey(BULK_LOAD_PREFIX, table.toString(), BLK_LD_DELIM));
      del.addFamily(BackupSystemTable.META_FAMILY);
      lstDels.add(del);
    }
    return lstDels;
  }

  private Put createPutForDeleteOperation(String[] backupIdList) {
    byte[] value = Bytes.toBytes(StringUtils.join(backupIdList, ","));
    Put put = new Put(DELETE_OP_ROW);
    put.addColumn(META_FAMILY, FAM_COL, value);
    return put;
  }

  private Delete createDeleteForBackupDeleteOperation() {
    Delete delete = new Delete(DELETE_OP_ROW);
    delete.addFamily(META_FAMILY);
    return delete;
  }

  private Get createGetForDeleteOperation() {
    Get get = new Get(DELETE_OP_ROW);
    get.addFamily(META_FAMILY);
    return get;
  }

  public void startDeleteOperation(String[] backupIdList) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Start delete operation for backups: " + StringUtils.join(backupIdList));
    }
    Put put = createPutForDeleteOperation(backupIdList);
    try (Table table = connection.getTable(tableName)) {
      table.put(put);
    }
  }

  public void finishDeleteOperation() throws IOException {
    LOG.trace("Finsih delete operation for backup ids");

    Delete delete = createDeleteForBackupDeleteOperation();
    try (Table table = connection.getTable(tableName)) {
      table.delete(delete);
    }
  }

  public String[] getListOfBackupIdsFromDeleteOperation() throws IOException {
    LOG.trace("Get delete operation for backup ids");

    Get get = createGetForDeleteOperation();
    try (Table table = connection.getTable(tableName)) {
      Result res = table.get(get);
      if (res.isEmpty()) {
        return null;
      }
      Cell cell = res.listCells().get(0);
      byte[] val = CellUtil.cloneValue(cell);
      if (val.length == 0) {
        return null;
      }
      return Splitter.on(',').splitToStream(new String(val, StandardCharsets.UTF_8))
        .toArray(String[]::new);
    }
  }

  private Put createPutForMergeOperation(String[] backupIdList) {
    byte[] value = Bytes.toBytes(StringUtils.join(backupIdList, ","));
    Put put = new Put(MERGE_OP_ROW);
    put.addColumn(META_FAMILY, FAM_COL, value);
    return put;
  }

  public boolean isMergeInProgress() throws IOException {
    Get get = new Get(MERGE_OP_ROW);
    try (Table table = connection.getTable(tableName)) {
      Result res = table.get(get);
      return !res.isEmpty();
    }
  }

  private Put createPutForUpdateTablesForMerge(List<TableName> tables) {
    byte[] value = Bytes.toBytes(StringUtils.join(tables, ","));
    Put put = new Put(MERGE_OP_ROW);
    put.addColumn(META_FAMILY, PATH_COL, value);
    return put;
  }

  private Delete createDeleteForBackupMergeOperation() {
    Delete delete = new Delete(MERGE_OP_ROW);
    delete.addFamily(META_FAMILY);
    return delete;
  }

  private Get createGetForMergeOperation() {
    Get get = new Get(MERGE_OP_ROW);
    get.addFamily(META_FAMILY);
    return get;
  }

  public void startMergeOperation(String[] backupIdList) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Start merge operation for backups: " + StringUtils.join(backupIdList));
    }
    Put put = createPutForMergeOperation(backupIdList);
    try (Table table = connection.getTable(tableName)) {
      table.put(put);
    }
  }

  public void updateProcessedTablesForMerge(List<TableName> tables) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Update tables for merge : " + StringUtils.join(tables, ","));
    }
    Put put = createPutForUpdateTablesForMerge(tables);
    try (Table table = connection.getTable(tableName)) {
      table.put(put);
    }
  }

  public void finishMergeOperation() throws IOException {
    LOG.trace("Finish merge operation for backup ids");

    Delete delete = createDeleteForBackupMergeOperation();
    try (Table table = connection.getTable(tableName)) {
      table.delete(delete);
    }
  }

  public String[] getListOfBackupIdsFromMergeOperation() throws IOException {
    LOG.trace("Get backup ids for merge operation");

    Get get = createGetForMergeOperation();
    try (Table table = connection.getTable(tableName)) {
      Result res = table.get(get);
      if (res.isEmpty()) {
        return null;
      }
      Cell cell = res.listCells().get(0);
      byte[] val = CellUtil.cloneValue(cell);
      if (val.length == 0) {
        return null;
      }
      return Splitter.on(',').splitToStream(new String(val, StandardCharsets.UTF_8))
        .toArray(String[]::new);
    }
  }

  /**
   * Creates a scan to read all registered bulk loads for the given table, or for all tables if
   * {@code table} is {@code null}.
   */
  static Scan createScanForOrigBulkLoadedFiles(@Nullable TableName table) {
    Scan scan = new Scan();
    byte[] startRow = table == null
      ? BULK_LOAD_PREFIX_BYTES
      : rowkey(BULK_LOAD_PREFIX, table.toString(), BLK_LD_DELIM);
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.withStartRow(startRow);
    scan.withStopRow(stopRow);
    scan.addFamily(BackupSystemTable.META_FAMILY);
    scan.readVersions(1);
    return scan;
  }

  static String getTableNameFromOrigBulkLoadRow(String rowStr) {
    // format is bulk : namespace : table : region : file
    return Iterators.get(Splitter.onPattern(BLK_LD_DELIM).split(rowStr).iterator(), 1);
  }

  static String getRegionNameFromOrigBulkLoadRow(String rowStr) {
    // format is bulk : namespace : table : region : file
    List<String> parts = Splitter.onPattern(BLK_LD_DELIM).splitToList(rowStr);
    Iterator<String> i = parts.iterator();
    int idx = 3;
    if (parts.size() == 4) {
      // the table is in default namespace
      idx = 2;
    }
    String region = Iterators.get(i, idx);
    LOG.debug("bulk row string " + rowStr + " region " + region);
    return region;
  }

  /*
   * Used to query bulk loaded hfiles which have been copied by incremental backup
   * @param backupId the backup Id. It can be null when querying for all tables
   * @return the Scan object
   * @deprecated This method is broken if a backupId is specified - see HBASE-28715
   */
  static Scan createScanForBulkLoadedFiles(String backupId) {
    Scan scan = new Scan();
    byte[] startRow =
      backupId == null ? BULK_LOAD_PREFIX_BYTES : rowkey(BULK_LOAD_PREFIX, backupId + BLK_LD_DELIM);
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.withStartRow(startRow);
    scan.withStopRow(stopRow);
    scan.addFamily(BackupSystemTable.META_FAMILY);
    scan.readVersions(1);
    return scan;
  }

  /**
   * Creates Scan operation to load backup set list
   * @return scan operation
   */
  private Scan createScanForBackupSetList() {
    Scan scan = new Scan();
    byte[] startRow = Bytes.toBytes(SET_KEY_PREFIX);
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.withStartRow(startRow);
    scan.withStopRow(stopRow);
    scan.addFamily(BackupSystemTable.META_FAMILY);
    return scan;
  }

  /**
   * Creates Get operation to load backup set content
   * @return get operation
   */
  private Get createGetForBackupSet(String name) {
    Get get = new Get(rowkey(SET_KEY_PREFIX, name));
    get.addFamily(BackupSystemTable.META_FAMILY);
    return get;
  }

  /**
   * Creates Delete operation to delete backup set content
   * @param name backup set's name
   * @return delete operation
   */
  private Delete createDeleteForBackupSet(String name) {
    Delete del = new Delete(rowkey(SET_KEY_PREFIX, name));
    del.addFamily(BackupSystemTable.META_FAMILY);
    return del;
  }

  /**
   * Creates Put operation to update backup set content
   * @param name   backup set's name
   * @param tables list of tables
   * @return put operation
   */
  private Put createPutForBackupSet(String name, String[] tables) {
    Put put = new Put(rowkey(SET_KEY_PREFIX, name));
    byte[] value = convertToByteArray(tables);
    put.addColumn(BackupSystemTable.META_FAMILY, Bytes.toBytes("tables"), value);
    return put;
  }

  private byte[] convertToByteArray(String[] tables) {
    return Bytes.toBytes(StringUtils.join(tables, ","));
  }

  /**
   * Converts cell to backup set list.
   * @param current current cell
   * @return backup set as array of table names
   */
  private String[] cellValueToBackupSet(Cell current) {
    byte[] data = CellUtil.cloneValue(current);
    if (!ArrayUtils.isEmpty(data)) {
      return Bytes.toString(data).split(",");
    }
    return new String[0];
  }

  /**
   * Converts cell key to backup set name.
   * @param current current cell
   * @return backup set name
   */
  private String cellKeyToBackupSetName(Cell current) {
    byte[] data = CellUtil.cloneRow(current);
    return Bytes.toString(data).substring(SET_KEY_PREFIX.length());
  }

  private static byte[] rowkey(String s, String... other) {
    StringBuilder sb = new StringBuilder(s);
    for (String ss : other) {
      sb.append(ss);
    }
    return Bytes.toBytes(sb.toString());
  }

  private static void ensureTableEnabled(Admin admin, TableName tableName) throws IOException {
    if (!admin.isTableEnabled(tableName)) {
      try {
        admin.enableTable(tableName);
      } catch (TableNotDisabledException ignored) {
        LOG.info("Table {} is not disabled, ignoring enable request", tableName);
      }
    }
  }
}
