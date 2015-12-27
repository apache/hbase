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
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;


/**
 * A collection for methods used by BackupSystemTable.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupSystemTableHelper {

  /**
   * hbase:backup schema: 
   * 1. Backup sessions rowkey= "session." + backupId; value = serialized
   * BackupContext 
   * 2. Backup start code rowkey = "startcode"; value = startcode 
   * 3. Incremental backup set rowkey="incrbackupset"; value=[list of tables] 
   * 4. Table-RS-timestamp map rowkey="trslm"+ table_name; value = map[RS-> last WAL timestamp] 
   * 5. RS - WAL ts map rowkey="rslogts."+server; value = last WAL timestamp 
   * 6. WALs recorded rowkey="wals."+WAL unique file name; value = NULL (value is not used)
   */
  private static final Log LOG = LogFactory.getLog(BackupSystemTableHelper.class);

  private final static String BACKUP_CONTEXT_PREFIX = "session.";
  private final static String START_CODE_ROW = "startcode";
  private final static String INCR_BACKUP_SET = "incrbackupset";
  private final static String TABLE_RS_LOG_MAP_PREFIX = "trslm.";
  private final static String RS_LOG_TS_PREFIX = "rslogts.";
  private final static String WALS_PREFIX = "wals.";

  private final static byte[] q0 = "0".getBytes();
  private final static byte[] EMPTY_VALUE = new byte[] {};

  private BackupSystemTableHelper() {
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Creates Put operation for a given backup context object
   * @param context backup context
   * @return put operation
   * @throws IOException exception
   */
  static Put createPutForBackupContext(BackupContext context) throws IOException {

    Put put = new Put((BACKUP_CONTEXT_PREFIX + context.getBackupId()).getBytes());
    put.addColumn(BackupSystemTable.familyName, q0, context.toByteArray());
    return put;
  }

  /**
   * Creates Get operation for a given backup id
   * @param backupId - backup's ID
   * @return get operation
   * @throws IOException exception
   */
  static Get createGetForBackupContext(String backupId) throws IOException {
    Get get = new Get((BACKUP_CONTEXT_PREFIX + backupId).getBytes());
    get.addFamily(BackupSystemTable.familyName);
    get.setMaxVersions(1);
    return get;
  }

  /**
   * Creates Delete operation for a given backup id
   * @param backupId - backup's ID
   * @return delete operation
   * @throws IOException exception
   */
  public static Delete createDeletForBackupContext(String backupId) {
    Delete del = new Delete((BACKUP_CONTEXT_PREFIX + backupId).getBytes());
    del.addFamily(BackupSystemTable.familyName);
    return del;
  }

  /**
   * Converts Result to BackupContext
   * @param res - HBase result
   * @return backup context instance
   * @throws IOException exception
   */
  static BackupContext resultToBackupContext(Result res) throws IOException {
    res.advance();
    Cell cell = res.current();
    return cellToBackupContext(cell);
  }

  /**
   * Creates Get operation to retrieve start code from hbase:backup
   * @return get operation
   * @throws IOException exception
   */
  static Get createGetForStartCode() throws IOException {
    Get get = new Get(START_CODE_ROW.getBytes());
    get.addFamily(BackupSystemTable.familyName);
    get.setMaxVersions(1);
    return get;
  }

  /**
   * Creates Put operation to store start code to hbase:backup
   * @return put operation
   * @throws IOException exception
   */
  static Put createPutForStartCode(String startCode) {
    Put put = new Put(START_CODE_ROW.getBytes());
    put.addColumn(BackupSystemTable.familyName, q0, startCode.getBytes());
    return put;
  }

  /**
   * Creates Get to retrieve incremental backup table set from hbase:backup
   * @return get operation
   * @throws IOException exception
   */
  static Get createGetForIncrBackupTableSet() throws IOException {
    Get get = new Get(INCR_BACKUP_SET.getBytes());
    get.addFamily(BackupSystemTable.familyName);
    get.setMaxVersions(1);
    return get;
  }

  /**
   * Creates Put to store incremental backup table set
   * @param tables tables
   * @return put operation
   */
  static Put createPutForIncrBackupTableSet(Set<String> tables) {
    Put put = new Put(INCR_BACKUP_SET.getBytes());
    for (String table : tables) {
      put.addColumn(BackupSystemTable.familyName, table.getBytes(), EMPTY_VALUE);
    }
    return put;
  }

  /**
   * Creates Scan operation to load backup history
   * @return scan operation
   */
  static Scan createScanForBackupHistory() {
    Scan scan = new Scan();
    byte[] startRow = BACKUP_CONTEXT_PREFIX.getBytes();
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.familyName);

    return scan;
  }

  /**
   * Converts cell to backup context instance.
   * @param current - cell
   * @return backup context instance
   * @throws IOException exception
   */
  static BackupContext cellToBackupContext(Cell current) throws IOException {
    byte[] data = CellUtil.cloneValue(current);
    try {
      BackupContext ctxt = BackupContext.fromByteArray(data);
      return ctxt;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates Put to write RS last roll log timestamp map
   * @param table - table
   * @param smap - map, containing RS:ts
   * @return put operation
   */
  static Put createPutForWriteRegionServerLogTimestamp(String table, String smap) {
    Put put = new Put((TABLE_RS_LOG_MAP_PREFIX + table).getBytes());
    put.addColumn(BackupSystemTable.familyName, q0, smap.getBytes());
    return put;
  }

  /**
   * Creates Scan to load table-> { RS -> ts} map of maps
   * @return scan operation
   */
  static Scan createScanForReadLogTimestampMap() {
    Scan scan = new Scan();
    byte[] startRow = TABLE_RS_LOG_MAP_PREFIX.getBytes();
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.familyName);

    return scan;
  }

  /**
   * Get table name from rowkey
   * @param cloneRow rowkey
   * @return table name
   */
  static String getTableNameForReadLogTimestampMap(byte[] cloneRow) {
    int prefixSize = TABLE_RS_LOG_MAP_PREFIX.length();
    return new String(cloneRow, prefixSize, cloneRow.length - prefixSize);
  }

  /**
   * Creates Put to store RS last log result
   * @param server - server name
   * @param fileName - log roll result (timestamp)
   * @return put operation
   */
  static Put createPutForRegionServerLastLogRollResult(String server, String fileName) {
    Put put = new Put((RS_LOG_TS_PREFIX + server).getBytes());
    put.addColumn(BackupSystemTable.familyName, q0, fileName.getBytes());
    return put;
  }

  /**
   * Creates Scan operation to load last RS log roll results
   * @return scan operation
   */
  static Scan createScanForReadRegionServerLastLogRollResult() {
    Scan scan = new Scan();
    byte[] startRow = RS_LOG_TS_PREFIX.getBytes();
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.familyName);

    return scan;
  }

  /**
   * Get server's name from rowkey
   * @param row - rowkey
   * @return server's name
   */
  static String getServerNameForReadRegionServerLastLogRollResult(byte[] row) {
    int prefixSize = RS_LOG_TS_PREFIX.length();
    return new String(row, prefixSize, row.length - prefixSize);
  }

  /**
   * Creates put list for list of WAL files
   * @param files list of WAL file paths
   * @param backupId backup id
   * @return put list
   * @throws IOException exception
   */
  public static List<Put> createPutsForAddWALFiles(List<String> files, String backupId)
      throws IOException {

    List<Put> puts = new ArrayList<Put>();
    for (String file : files) {
      LOG.debug("+++ put: " + BackupUtil.getUniqueWALFileNamePart(file));
      byte[] row = (WALS_PREFIX + BackupUtil.getUniqueWALFileNamePart(file)).getBytes();
      Put put = new Put(row);
      put.addColumn(BackupSystemTable.familyName, q0, backupId.getBytes());
      puts.add(put);
    }
    return puts;
  }

  /**
   * Creates Get operation for a given wal file name
   * @param file file
   * @return get operation
   * @throws IOException exception
   */
  public static Get createGetForCheckWALFile(String file) throws IOException {
    byte[] row = (WALS_PREFIX + BackupUtil.getUniqueWALFileNamePart(file)).getBytes();
    Get get = new Get(row);
    get.addFamily(BackupSystemTable.familyName);
    get.setMaxVersions(1);
    return get;
  }

}
