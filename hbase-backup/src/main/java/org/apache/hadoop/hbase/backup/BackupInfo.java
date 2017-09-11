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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.BackupInfo.Builder;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * An object to encapsulate the information for each backup session
 */
@InterfaceAudience.Private
public class BackupInfo implements Comparable<BackupInfo> {
  private static final Log LOG = LogFactory.getLog(BackupInfo.class);

  public static interface Filter {

    /**
     * Filter interface
     * @param info backup info
     * @return true if info passes filter, false otherwise
     */
    public boolean apply(BackupInfo info);
  }

  /**
   * Backup session states
   */
  public static enum BackupState {
    RUNNING, COMPLETE, FAILED, ANY;
  }

  /**
   * BackupPhase - phases of an ACTIVE backup session (running), when state of a backup session is
   * BackupState.RUNNING
   */
  public static enum BackupPhase {
    REQUEST, SNAPSHOT, PREPARE_INCREMENTAL, SNAPSHOTCOPY, INCREMENTAL_COPY, STORE_MANIFEST;
  }

  /**
   * Backup id
   */
  private String backupId;

  /**
   * Backup type, full or incremental
   */
  private BackupType type;

  /**
   * Target root directory for storing the backup files
   */
  private String backupRootDir;

  /**
   * Backup state
   */
  private BackupState state;

  /**
   * Backup phase
   */
  private BackupPhase phase = BackupPhase.REQUEST;

  /**
   * Backup failure message
   */
  private String failedMsg;

  /**
   * Backup status map for all tables
   */
  private Map<TableName, BackupTableInfo> backupTableInfoMap;

  /**
   * Actual start timestamp of a backup process
   */
  private long startTs;

  /**
   * Actual end timestamp of the backup process
   */
  private long completeTs;

  /**
   * Total bytes of incremental logs copied
   */
  private long totalBytesCopied;

  /**
   * For incremental backup, a location of a backed-up hlogs
   */
  private String hlogTargetDir = null;

  /**
   * Incremental backup file list
   */
  private List<String> incrBackupFileList;

  /**
   * New region server log timestamps for table set after distributed log roll key - table name,
   * value - map of RegionServer hostname -> last log rolled timestamp
   */
  private HashMap<TableName, HashMap<String, Long>> tableSetTimestampMap;

  /**
   * Backup progress in %% (0-100)
   */
  private int progress;

  /**
   * Number of parallel workers. -1 - system defined
   */
  private int workers = -1;

  /**
   * Bandwidth per worker in MB per sec. -1 - unlimited
   */
  private long bandwidth = -1;

  public BackupInfo() {
    backupTableInfoMap = new HashMap<TableName, BackupTableInfo>();
  }

  public BackupInfo(String backupId, BackupType type, TableName[] tables, String targetRootDir) {
    this();
    this.backupId = backupId;
    this.type = type;
    this.backupRootDir = targetRootDir;
    this.addTables(tables);
    if (type == BackupType.INCREMENTAL) {
      setHLogTargetDir(BackupUtils.getLogBackupDir(targetRootDir, backupId));
    }
    this.startTs = 0;
    this.completeTs = 0;
  }

  public int getWorkers() {
    return workers;
  }

  public void setWorkers(int workers) {
    this.workers = workers;
  }

  public long getBandwidth() {
    return bandwidth;
  }

  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  public void setBackupTableInfoMap(Map<TableName, BackupTableInfo> backupTableInfoMap) {
    this.backupTableInfoMap = backupTableInfoMap;
  }

  public HashMap<TableName, HashMap<String, Long>> getTableSetTimestampMap() {
    return tableSetTimestampMap;
  }

  public void setTableSetTimestampMap(HashMap<TableName,
                                      HashMap<String, Long>> tableSetTimestampMap) {
    this.tableSetTimestampMap = tableSetTimestampMap;
  }

  public void setType(BackupType type) {
    this.type = type;
  }

  public void setBackupRootDir(String targetRootDir) {
    this.backupRootDir = targetRootDir;
  }

  public void setTotalBytesCopied(long totalBytesCopied) {
    this.totalBytesCopied = totalBytesCopied;
  }

  /**
   * Set progress (0-100%)
   * @param p progress value
   */

  public void setProgress(int p) {
    this.progress = p;
  }

  /**
   * Get current progress
   */
  public int getProgress() {
    return progress;
  }

  public String getBackupId() {
    return backupId;
  }

  public void setBackupId(String backupId) {
    this.backupId = backupId;
  }

  public BackupTableInfo getBackupTableInfo(TableName table) {
    return this.backupTableInfoMap.get(table);
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

  public long getCompleteTs() {
    return completeTs;
  }

  public void setCompleteTs(long endTs) {
    this.completeTs = endTs;
  }

  public long getTotalBytesCopied() {
    return totalBytesCopied;
  }

  public BackupState getState() {
    return state;
  }

  public void setState(BackupState flag) {
    this.state = flag;
  }

  public BackupPhase getPhase() {
    return phase;
  }

  public void setPhase(BackupPhase phase) {
    this.phase = phase;
  }

  public BackupType getType() {
    return type;
  }

  public void setSnapshotName(TableName table, String snapshotName) {
    this.backupTableInfoMap.get(table).setSnapshotName(snapshotName);
  }

  public String getSnapshotName(TableName table) {
    return this.backupTableInfoMap.get(table).getSnapshotName();
  }

  public List<String> getSnapshotNames() {
    List<String> snapshotNames = new ArrayList<String>();
    for (BackupTableInfo backupStatus : this.backupTableInfoMap.values()) {
      snapshotNames.add(backupStatus.getSnapshotName());
    }
    return snapshotNames;
  }

  public Set<TableName> getTables() {
    return this.backupTableInfoMap.keySet();
  }

  public List<TableName> getTableNames() {
    return new ArrayList<TableName>(backupTableInfoMap.keySet());
  }

  public void addTables(TableName[] tables) {
    for (TableName table : tables) {
      BackupTableInfo backupStatus = new BackupTableInfo(table, this.backupRootDir, this.backupId);
      this.backupTableInfoMap.put(table, backupStatus);
    }
  }

  public void setTables(List<TableName> tables) {
    this.backupTableInfoMap.clear();
    for (TableName table : tables) {
      BackupTableInfo backupStatus = new BackupTableInfo(table, this.backupRootDir, this.backupId);
      this.backupTableInfoMap.put(table, backupStatus);
    }
  }

  public String getBackupRootDir() {
    return backupRootDir;
  }

  public String getTableBackupDir(TableName tableName) {
    return BackupUtils.getTableBackupDir(backupRootDir, backupId, tableName);
  }

  public void setHLogTargetDir(String hlogTagetDir) {
    this.hlogTargetDir = hlogTagetDir;
  }

  public String getHLogTargetDir() {
    return hlogTargetDir;
  }

  public List<String> getIncrBackupFileList() {
    return incrBackupFileList;
  }

  public void setIncrBackupFileList(List<String> incrBackupFileList) {
    this.incrBackupFileList = incrBackupFileList;
  }

  /**
   * Set the new region server log timestamps after distributed log roll
   * @param newTableSetTimestampMap table timestamp map
   */
  public void
      setIncrTimestampMap(HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap) {
    this.tableSetTimestampMap = newTableSetTimestampMap;
  }

  /**
   * Get new region server log timestamps after distributed log roll
   * @return new region server log timestamps
   */
  public HashMap<TableName, HashMap<String, Long>> getIncrTimestampMap() {
    return this.tableSetTimestampMap;
  }

  public TableName getTableBySnapshot(String snapshotName) {
    for (Entry<TableName, BackupTableInfo> entry : this.backupTableInfoMap.entrySet()) {
      if (snapshotName.equals(entry.getValue().getSnapshotName())) {
        return entry.getKey();
      }
    }
    return null;
  }

  public BackupProtos.BackupInfo toProtosBackupInfo() {
    BackupProtos.BackupInfo.Builder builder = BackupProtos.BackupInfo.newBuilder();
    builder.setBackupId(getBackupId());
    setBackupTableInfoMap(builder);
    builder.setCompleteTs(getCompleteTs());
    if (getFailedMsg() != null) {
      builder.setFailedMessage(getFailedMsg());
    }
    if (getState() != null) {
      builder.setBackupState(BackupProtos.BackupInfo.BackupState.valueOf(getState().name()));
    }
    if (getPhase() != null) {
      builder.setBackupPhase(BackupProtos.BackupInfo.BackupPhase.valueOf(getPhase().name()));
    }

    builder.setProgress(getProgress());
    builder.setStartTs(getStartTs());
    builder.setBackupRootDir(getBackupRootDir());
    builder.setBackupType(BackupProtos.BackupType.valueOf(getType().name()));
    builder.setWorkersNumber(workers);
    builder.setBandwidth(bandwidth);
    return builder.build();
  }

  @Override
  public int hashCode() {
    int hash = 33 * type.hashCode() + backupId != null ? backupId.hashCode() : 0;
    if (backupRootDir != null) {
      hash = 33 * hash + backupRootDir.hashCode();
    }
    hash = 33 * hash + state.hashCode();
    hash = 33 * hash + phase.hashCode();
    hash = 33 * hash + (int) (startTs ^ (startTs >>> 32));
    hash = 33 * hash + (int) (completeTs ^ (completeTs >>> 32));
    hash = 33 * hash + (int) (totalBytesCopied ^ (totalBytesCopied >>> 32));
    if (hlogTargetDir != null) {
      hash = 33 * hash + hlogTargetDir.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BackupInfo) {
      BackupInfo other = (BackupInfo) obj;
      try {
        return Bytes.equals(toByteArray(), other.toByteArray());
      } catch (IOException e) {
        LOG.error(e);
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return backupId;
  }

  public byte[] toByteArray() throws IOException {
    return toProtosBackupInfo().toByteArray();
  }

  private void setBackupTableInfoMap(Builder builder) {
    for (Entry<TableName, BackupTableInfo> entry : backupTableInfoMap.entrySet()) {
      builder.addBackupTableInfo(entry.getValue().toProto());
    }
  }

  public static BackupInfo fromByteArray(byte[] data) throws IOException {
    return fromProto(BackupProtos.BackupInfo.parseFrom(data));
  }

  public static BackupInfo fromStream(final InputStream stream) throws IOException {
    return fromProto(BackupProtos.BackupInfo.parseDelimitedFrom(stream));
  }

  public static BackupInfo fromProto(BackupProtos.BackupInfo proto) {
    BackupInfo context = new BackupInfo();
    context.setBackupId(proto.getBackupId());
    context.setBackupTableInfoMap(toMap(proto.getBackupTableInfoList()));
    context.setCompleteTs(proto.getCompleteTs());
    if (proto.hasFailedMessage()) {
      context.setFailedMsg(proto.getFailedMessage());
    }
    if (proto.hasBackupState()) {
      context.setState(BackupInfo.BackupState.valueOf(proto.getBackupState().name()));
    }

    context.setHLogTargetDir(BackupUtils.getLogBackupDir(proto.getBackupRootDir(),
      proto.getBackupId()));

    if (proto.hasBackupPhase()) {
      context.setPhase(BackupPhase.valueOf(proto.getBackupPhase().name()));
    }
    if (proto.hasProgress()) {
      context.setProgress(proto.getProgress());
    }
    context.setStartTs(proto.getStartTs());
    context.setBackupRootDir(proto.getBackupRootDir());
    context.setType(BackupType.valueOf(proto.getBackupType().name()));
    context.setWorkers(proto.getWorkersNumber());
    context.setBandwidth(proto.getBandwidth());
    return context;
  }

  private static Map<TableName, BackupTableInfo> toMap(List<BackupProtos.BackupTableInfo> list) {
    HashMap<TableName, BackupTableInfo> map = new HashMap<>();
    for (BackupProtos.BackupTableInfo tbs : list) {
      map.put(ProtobufUtil.toTableName(tbs.getTableName()), BackupTableInfo.convert(tbs));
    }
    return map;
  }

  public String getShortDescription() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("ID=" + backupId).append(",");
    sb.append("Type=" + getType()).append(",");
    sb.append("Tables=" + getTableListAsString()).append(",");
    sb.append("State=" + getState()).append(",");
    Date date = null;
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(getStartTs());
    date = cal.getTime();
    sb.append("Start time=" + date).append(",");
    if (state == BackupState.FAILED) {
      sb.append("Failed message=" + getFailedMsg()).append(",");
    } else if (state == BackupState.RUNNING) {
      sb.append("Phase=" + getPhase()).append(",");
    } else if (state == BackupState.COMPLETE) {
      cal = Calendar.getInstance();
      cal.setTimeInMillis(getCompleteTs());
      date = cal.getTime();
      sb.append("End time=" + date).append(",");
    }
    sb.append("Progress=" + getProgress() + "%");
    sb.append("}");

    return sb.toString();
  }

  public String getStatusAndProgressAsString() {
    StringBuilder sb = new StringBuilder();
    sb.append("id: ").append(getBackupId()).append(" state: ").append(getState())
        .append(" progress: ").append(getProgress());
    return sb.toString();
  }

  public String getTableListAsString() {
    StringBuffer sb = new StringBuffer();
    sb.append("{");
    sb.append(StringUtils.join(backupTableInfoMap.keySet(), ","));
    sb.append("}");
    return sb.toString();
  }

  /**
   * We use only time stamps to compare objects during sort operation
   */
  @Override
  public int compareTo(BackupInfo o) {
    Long thisTS =
        Long.valueOf(this.getBackupId().substring(this.getBackupId().lastIndexOf("_") + 1));
    Long otherTS = Long.valueOf(o.getBackupId().substring(o.getBackupId().lastIndexOf("_") + 1));
    return thisTS.compareTo(otherTS);
  }

}
