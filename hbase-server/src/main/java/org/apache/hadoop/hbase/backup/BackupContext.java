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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * An object to encapsulate the information for each backup request
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupContext implements Serializable {

  private static final long serialVersionUID = 2401435114454300992L;

  // backup id: a timestamp when we request the backup
  private String backupId;

  // backup type, full or incremental
  private String type;

  // target root directory for storing the backup files
  private String targetRootDir;

  // overall backup status
  private BackupHandler.BACKUPSTATUS flag;

  // overall backup phase
  private BackupHandler.BACKUPPHASE phase;

  // overall backup failure message
  private String failedMsg;

  // backup status map for all tables
  private Map<String, BackupStatus> backupStatusMap;

  // actual start timestamp of the backup process
  private long startTs;

  // actual end timestamp of the backup process, could be fail or complete
  private long endTs;

  // the total bytes of incremental logs copied
  private long totalBytesCopied;

  // for incremental backup, the location of the backed-up hlogs
  private String hlogTargetDir = null;

  // incremental backup file list
  transient private List<String> incrBackupFileList;

  // new region server log timestamps for table set after distributed log roll
  // key - table name, value - map of RegionServer hostname -> last log rolled timestamp
  transient private HashMap<String, HashMap<String, String>> tableSetTimestampMap;

  // cancel flag
  private boolean cancelled = false;
  // backup progress string

  private String progress;

  public BackupContext() {
  }

  public BackupContext(String backupId, String type, String[] tables, String targetRootDir,
      String snapshot) {
    super();

    if (backupStatusMap == null) {
      backupStatusMap = new HashMap<String, BackupStatus>();
    }

    this.backupId = backupId;
    this.type = type;
    this.targetRootDir = targetRootDir;

    this.addTables(tables);

    if (type.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
      setHlogTargetDir(HBackupFileSystem.getLogBackupDir(targetRootDir, backupId));
    }

    this.startTs = 0;
    this.endTs = 0;

  }

  /**
   * Set progress string
   * @param msg progress message
   */

  public void setProgress(String msg) {
    this.progress = msg;
  }

  /**
   * Get current progress msg
   */
  public String getProgress() {
    return progress;
  }

  /**
   * Mark cancel flag.
   */
  public void markCancel() {
    this.cancelled = true;
  }

  /**
   * Has been marked as cancelled or not.
   * @return True if marked as cancelled
   */
  public boolean isCancelled() {
    return this.cancelled;
  }

  public String getBackupId() {
    return backupId;
  }

  public void setBackupId(String backupId) {
    this.backupId = backupId;
  }

  public BackupStatus getBackupStatus(String table) {
    return this.backupStatusMap.get(table);
  }

  public String getFailedMsg() {
    return failedMsg;
  }

  public void setFailedMsg(String failedMsg) {
    this.failedMsg = failedMsg;
  }

  public long getStartTs() {
    return startTs;
  }

  public void setStartTs(long startTs) {
    this.startTs = startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public void setEndTs(long endTs) {
    this.endTs = endTs;
  }

  public long getTotalBytesCopied() {
    return totalBytesCopied;
  }

  public BackupHandler.BACKUPSTATUS getFlag() {
    return flag;
  }

  public void setFlag(BackupHandler.BACKUPSTATUS flag) {
    this.flag = flag;
  }

  public BackupHandler.BACKUPPHASE getPhase() {
    return phase;
  }

  public void setPhase(BackupHandler.BACKUPPHASE phase) {
    this.phase = phase;
  }

  public String getType() {
    return type;
  }

  public void setSnapshotName(String table, String snapshotName) {
    this.backupStatusMap.get(table).setSnapshotName(snapshotName);
  }

  public String getSnapshotName(String table) {
    return this.backupStatusMap.get(table).getSnapshotName();
  }

  public List<String> getSnapshotNames() {
    List<String> snapshotNames = new ArrayList<String>();
    for (BackupStatus backupStatus : this.backupStatusMap.values()) {
      snapshotNames.add(backupStatus.getSnapshotName());
    }
    return snapshotNames;
  }

  public Set<String> getTables() {
    return this.backupStatusMap.keySet();
  }

  public String getTableListAsString() {
    return BackupUtil.concat(backupStatusMap.keySet(), ";");
  }

  public void addTables(String[] tables) {
    for (String table : tables) {
      BackupStatus backupStatus = new BackupStatus(table, this.targetRootDir, this.backupId);
      this.backupStatusMap.put(table, backupStatus);
    }
  }

  public String getTargetRootDir() {
    return targetRootDir;
  }

  public void setHlogTargetDir(String hlogTagetDir) {
    this.hlogTargetDir = hlogTagetDir;
  }

  public String getHLogTargetDir() {
    return hlogTargetDir;
  }

  public List<String> getIncrBackupFileList() {
    return incrBackupFileList;
  }

  public List<String> setIncrBackupFileList(List<String> incrBackupFileList) {
    this.incrBackupFileList = incrBackupFileList;
    return this.incrBackupFileList;
  }

  /**
   * Set the new region server log timestamps after distributed log roll
   * @param newTableSetTimestampMap table timestamp map
   */
  public void setIncrTimestampMap(HashMap<String, 
      HashMap<String, String>> newTableSetTimestampMap) {
    this.tableSetTimestampMap = newTableSetTimestampMap;
  }

  /**
   * Get new region server log timestamps after distributed log roll
   * @return new region server log timestamps
   */
  public HashMap<String, HashMap<String, String>> getIncrTimestampMap() {
    return this.tableSetTimestampMap;
  }

  /**
   * Get existing snapshot if backing up from existing snapshot.
   * @return The existing snapshot, null if not backing up from existing snapshot
   */
  public String getExistingSnapshot() {
    // this feature will be supported in another Jira
    return null;
  }

  /**
   * Check whether this backup context are for backing up from existing snapshot or not.
   * @return true if it is for backing up from existing snapshot, otherwise false
   */
  public boolean fromExistingSnapshot() {
    // this feature will be supported in later jiras
    return false;
  }

  public String getTableBySnapshot(String snapshotName) {
    for (Entry<String, BackupStatus> entry : this.backupStatusMap.entrySet()) {
      if (snapshotName.equals(entry.getValue().getSnapshotName())) {
        return entry.getKey();
      }
    }
    return null;
  }

  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(this);
    return baos.toByteArray();
  }

  public static BackupContext fromByteArray(byte[] data) 
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    return (BackupContext) ois.readObject();
  }
}
