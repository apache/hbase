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

import java.io.EOFException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitter;

/**
 * A collection for methods used by multiple classes to restore HBase tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RestoreUtil {

  public static final Log LOG = LogFactory.getLog(RestoreUtil.class);

  protected Configuration conf = null;

  protected HBackupFileSystem hBackupFS = null;

  // store table name and snapshot dir mapping
  private final HashMap<String, Path> snapshotMap = new HashMap<String, Path>();

  public RestoreUtil(Configuration conf, HBackupFileSystem hBackupFS) throws IOException {
    this.conf = conf;
    this.hBackupFS = hBackupFS;
  }

  /**
   * During incremental backup operation. Call WalPlayer to replay WAL in backup image Currently
   * tableNames and newTablesNames only contain single table, will be expanded to multiple tables in
   * the future
   * @param logDir : incremental backup folders, which contains WAL
   * @param tableNames : source tableNames(table names were backuped)
   * @param newTableNames : target tableNames(table names to be restored to)
   * @throws IOException exception
   */
  public void incrementalRestoreTable(String logDir, String[] tableNames, String[] newTableNames)
      throws IOException {

    if (tableNames.length != newTableNames.length) {
      throw new IOException("Number of source tables adn taget Tables does not match!");
    }

    // for incremental backup image, expect the table already created either by user or previous
    // full backup. Here, check that all new tables exists
    HBaseAdmin admin = null;
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = (HBaseAdmin) conn.getAdmin();
      for (String tableName : newTableNames) {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
          admin.close();
          throw new IOException("HBase table " + tableName
            + " does not exist. Create the table first, e.g. by restoring a full backup.");
        }
      }
      IncrementalRestoreService restoreService =
          BackupRestoreServiceFactory.getIncrementalRestoreService(conf);

      restoreService.run(logDir, tableNames, newTableNames);
    } finally {
      if (admin != null) {
        admin.close();
      }
      if(conn != null){
        conn.close();
      }
    }
  }

  public void fullRestoreTable(Path tableBackupPath, String tableName, String newTableName,
      boolean converted) throws IOException {

    restoreTableAndCreate(tableName, newTableName, tableBackupPath, converted);
  }

  private void restoreTableAndCreate(String tableName, String newTableName, Path tableBackupPath,
       boolean converted) throws IOException {
    if (newTableName == null || newTableName.equals("")) {
      newTableName = tableName;
    }

    FileSystem fileSys = tableBackupPath.getFileSystem(this.conf);

    // get table descriptor first
    HTableDescriptor tableDescriptor = null;

    Path tableSnapshotPath = hBackupFS.getTableSnapshotPath(tableName);

    if (fileSys.exists(tableSnapshotPath)) {
      // snapshot path exist means the backup path is in HDFS
      // check whether snapshot dir already recorded for target table
      if (snapshotMap.get(tableName) != null) {
        SnapshotDescription desc =
            SnapshotDescriptionUtils.readSnapshotInfo(fileSys, tableSnapshotPath);
        SnapshotManifest manifest = SnapshotManifest.open(conf, fileSys, tableSnapshotPath, desc);
        tableDescriptor = manifest.getTableDescriptor();
        LOG.debug("tableDescriptor.getNameAsString() = " + tableDescriptor.getNameAsString()
          + " while tableName = " + tableName);
        // HBase 96.0 and 98.0
        // tableDescriptor =
        // FSTableDescriptors.getTableDescriptorFromFs(fileSys, snapshotMap.get(tableName));
      } else {
        tableDescriptor = hBackupFS.getTableDesc(tableName);
        LOG.debug("tableSnapshotPath=" + tableSnapshotPath.toString());
        snapshotMap.put(tableName, hBackupFS.getTableInfoPath(tableName));
      }
      if (tableDescriptor == null) {
        LOG.debug("Found no table descriptor in the snapshot dir, previous schema would be lost");
      }
    } else if (converted) {
      // first check if this is a converted backup image
      LOG.error("convert will be supported in a future jira");
    }

    Path tableArchivePath = hBackupFS.getTableArchivePath(tableName);
    if (tableArchivePath == null) {
      if (tableDescriptor != null) {
        // find table descriptor but no archive dir means the table is empty, create table and exit
        LOG.debug("find table descriptor but no archive dir for table " + tableName
          + ", will only create table");
        tableDescriptor.setName(Bytes.toBytes(newTableName));
        checkAndCreateTable(tableBackupPath, tableName, newTableName, null, tableDescriptor);
        return;
      } else {
        throw new IllegalStateException("Cannot restore hbase table because directory '"
            + " tableArchivePath is null.");
      }
    }

    if (tableDescriptor == null) {
      tableDescriptor = new HTableDescriptor(newTableName);
    } else {
      tableDescriptor.setName(Bytes.toBytes(newTableName));
    }

    if (!converted) {
      // record all region dirs:
      // load all files in dir
      try {
        ArrayList<Path> regionPathList = hBackupFS.getRegionList(tableName);

        // should only try to create the table with all region informations, so we could pre-split
        // the regions in fine grain
        checkAndCreateTable(tableBackupPath, tableName, newTableName, regionPathList,
          tableDescriptor);
        if (tableArchivePath != null) {
          // start real restore through bulkload
          // if the backup target is on local cluster, special action needed
          Path tempTableArchivePath = hBackupFS.checkLocalAndBackup(tableArchivePath);
          if (tempTableArchivePath.equals(tableArchivePath)) {
            LOG.debug("TableArchivePath for bulkload using existPath: " + tableArchivePath);
          } else {
            regionPathList = hBackupFS.getRegionList(tempTableArchivePath); // point to the tempDir
            LOG.debug("TableArchivePath for bulkload using tempPath: " + tempTableArchivePath);
          }

          LoadIncrementalHFiles loader = createLoader(tempTableArchivePath, false);
          for (Path regionPath : regionPathList) {
            String regionName = regionPath.toString();
            LOG.debug("Restoring HFiles from directory " + regionName);
            String[] args = { regionName, newTableName };
            loader.run(args);
          }
        }
        // restore the recovered.edits if exists
        replayRecoveredEditsIfAny(tableBackupPath, tableName, tableDescriptor);
      } catch (Exception e) {
        throw new IllegalStateException("Cannot restore hbase table", e);
      }
    } else {
      LOG.debug("convert will be supported in a future jira");
    }
  }

  /**
   * Replay recovered edits from backup.
   */
  private void replayRecoveredEditsIfAny(Path tableBackupPath, String tableName,
      HTableDescriptor newTableHtd) throws IOException {

    LOG.debug("Trying to replay the recovered.edits if exist to the target table "
        + newTableHtd.getNameAsString() + " from the backup of table " + tableName + ".");

    FileSystem fs = tableBackupPath.getFileSystem(this.conf);
    ArrayList<Path> regionDirs = hBackupFS.getRegionList(tableName);

    if (regionDirs == null || regionDirs.size() == 0) {
      LOG.warn("No recovered.edits to be replayed for empty backup of table " + tableName + ".");
      return;
    }

    Connection conn = null;
    try {

      conn = ConnectionFactory.createConnection(conf);

      for (Path regionDir : regionDirs) {
        // OLD: NavigableSet<Path> files = HLogUtil.getSplitEditFilesSorted(fs, regionDir);
        NavigableSet<Path> files = WALSplitter.getSplitEditFilesSorted(fs, regionDir);

        if (files == null || files.isEmpty()) {
          LOG.warn("No recovered.edits found for the region " + regionDir.getName() + ".");
          return;
        }

        for (Path edits : files) {
          if (edits == null || !fs.exists(edits)) {
            LOG.warn("Null or non-existent edits file: " + edits);
            continue;
          }

          HTable table = null;
          try {
            table = (HTable) conn.getTable(newTableHtd.getTableName());
            replayRecoveredEdits(table, fs, edits);
            table.flushCommits();
            table.close();
          } catch (IOException e) {
            boolean skipErrors = conf.getBoolean("hbase.skip.errors", false);
            if (skipErrors) {
              Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
              LOG.error(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS
                + "=true so continuing. Renamed " + edits + " as " + p, e);
            } else {
              throw e;
            }
          } finally {
            if (table != null) {
              table.close();
            }
          }
        } // for each edit file under a region
      } // for each region

    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Restore process for an edit entry.
   * @param htable The target table of restore
   * @param key HLog key
   * @param val KVs
   * @throws IOException exception
   */
  private void restoreEdit(HTable htable, WALKey key, WALEdit val) throws IOException {
    Put put = null;
    Delete del = null;
    Cell lastKV = null;
    for (Cell kv : val.getCells()) {
      // filtering HLog meta entries, see HLog.completeCacheFlushLogEdit
      if (WALEdit.isMetaEditFamily(CellUtil.cloneFamily(kv))) {
        continue;
      }

      // A WALEdit may contain multiple operations (HBASE-3584) and/or
      // multiple rows (HBASE-5229).
      // Aggregate as much as possible into a single Put/Delete
      // operation before apply the action to the table.
      if (lastKV == null || lastKV.getTypeByte() != kv.getTypeByte()
          || !CellUtil.matchingRow(lastKV, kv)) {
        // row or type changed, write out aggregate KVs.
        if (put != null) {
          applyAction(htable, put);
        }
        if (del != null) {
          applyAction(htable, del);
        }

        if (CellUtil.isDelete(kv)) {
          del = new Delete(CellUtil.cloneRow(kv));
        } else {
          put = new Put(CellUtil.cloneRow(kv));
        }
      }
      if (CellUtil.isDelete(kv)) {
        del.addDeleteMarker(kv);
      } else {
        put.add(kv);
      }
      lastKV = kv;
    }
    // write residual KVs
    if (put != null) {
      applyAction(htable, put);
    }
    if (del != null) {
      applyAction(htable, del);
    }
  }

  /**
   * Apply an action (Put/Delete) to table.
   * @param table table
   * @param action action
   * @throws IOException exception
   */
  private void applyAction(HTable table, Mutation action) throws IOException {
    // The actions are not immutable, so we defensively copy them
    if (action instanceof Put) {
      Put put = new Put((Put) action);
      // put.setWriteToWAL(false);
      // why do not we do WAL?
      put.setDurability(Durability.SKIP_WAL);
      table.put(put);
    } else if (action instanceof Delete) {
      Delete delete = new Delete((Delete) action);
      table.delete(delete);
    } else {
      throw new IllegalArgumentException("action must be either Delete or Put");
    }
  }

  /**
   * Replay the given edits.
   * @param htable The target table of restore
   * @param fs File system
   * @param edits Recovered.edits to be replayed
   * @throws IOException exception
   */
  private void replayRecoveredEdits(HTable htable, FileSystem fs, Path edits) throws IOException {
    LOG.debug("Replaying edits from " + edits + "; path=" + edits);

    WAL.Reader reader = null;
    try {
      reader = WALFactory.createReader(fs, edits, this.conf);
      long editsCount = 0;
      WAL.Entry entry;

      try {
        while ((entry = reader.next()) != null) {
          restoreEdit(htable, entry.getKey(), entry.getEdit());
          editsCount++;
        }
        LOG.debug(editsCount + " edits from " + edits + " have been replayed.");

      } catch (EOFException eof) {
        Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
        String msg =
            "Encountered EOF. Most likely due to Master failure during "
                + "log spliting, so we have this data in another edit.  "
                + "Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitter.moveAsideBadEditsFile(fs, edits);
          String msg =
              "File corruption encountered!  " + "Continuing, but renaming " + edits + " as " + p;
          LOG.warn(msg, ioe);
        } else {
          // other IO errors may be transient (bad network connection,
          // checksum exception on one datanode, etc). throw & retry
          throw ioe;
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Create a {@link LoadIncrementalHFiles} instance to be used to restore the HFiles of a full
   * backup.
   * @return the {@link LoadIncrementalHFiles} instance
   * @throws IOException exception
   */
  private LoadIncrementalHFiles createLoader(Path tableArchivePath, boolean multipleTables)
      throws IOException {
    // set configuration for restore:
    // LoadIncrementalHFile needs more time
    // <name>hbase.rpc.timeout</name> <value>600000</value>
    // calculates
    Integer milliSecInMin = 60000;
    Integer previousMillis = this.conf.getInt("hbase.rpc.timeout", 0);
    Integer numberOfFilesInDir =
        multipleTables ? hBackupFS.getMaxNumberOfFilesInSubDir(tableArchivePath) : hBackupFS
            .getNumberOfFilesInDir(tableArchivePath);
    Integer calculatedMillis = numberOfFilesInDir * milliSecInMin; // 1 minute per file
    Integer resultMillis = Math.max(calculatedMillis, previousMillis);
    if (resultMillis > previousMillis) {
      LOG.info("Setting configuration for restore with LoadIncrementalHFile: "
          + "hbase.rpc.timeout to " + calculatedMillis / milliSecInMin
          + " minutes, to handle the number of files in backup " + tableArchivePath);
      this.conf.setInt("hbase.rpc.timeout", resultMillis);
    }

    LoadIncrementalHFiles loader = null;
    try {
      loader = new LoadIncrementalHFiles(this.conf);
    } catch (Exception e1) {
      throw new IOException(e1);
    }
    return loader;
  }

  /**
   * Prepare the table for bulkload, most codes copied from
   * {@link LoadIncrementalHFiles#createTable(String, String)}
   * @param tableBackupPath path
   * @param tableName table name
   * @param targetTableName target table name
   * @param regionDirList region directory list
   * @param htd table descriptor
   * @throws IOException exception
   */
  private void checkAndCreateTable(Path tableBackupPath, String tableName, String targetTableName,
      ArrayList<Path> regionDirList, HTableDescriptor htd) throws IOException {
    HBaseAdmin hbadmin = null;
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      hbadmin = (HBaseAdmin) conn.getAdmin();
      if (hbadmin.tableExists(TableName.valueOf(targetTableName))) {
        LOG.info("Using exising target table '" + targetTableName + "'");
      } else {
        LOG.info("Creating target table '" + targetTableName + "'");

        // if no region dir given, create the table and return
        if (regionDirList == null || regionDirList.size() == 0) {

          hbadmin.createTable(htd);
          return;
        }

        byte[][] keys = hBackupFS.generateBoundaryKeys(regionDirList);

        // create table using table decriptor and region boundaries
        hbadmin.createTable(htd, keys);
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (hbadmin != null) {
        hbadmin.close();
      }
      if(conn != null){
        conn.close();
      }
    }
  }

}
