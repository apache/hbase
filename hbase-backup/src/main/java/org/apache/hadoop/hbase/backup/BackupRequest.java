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

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * POJO class for backup request
 */
@InterfaceAudience.Private
public final class BackupRequest {

  public static class Builder {

    BackupRequest request;

    public Builder() {
      request = new BackupRequest();
    }

    public Builder withBackupType(BackupType type) {
      request.setBackupType(type);
      return this;
    }

    public Builder withTableList(List<TableName> tables) {
      request.setTableList(tables);
      return this;
    }

    public Builder withTargetRootDir(String backupDir) {
      request.setTargetRootDir(backupDir);
      return this;
    }

    public Builder withBackupSetName(String setName) {
      request.setBackupSetName(setName);
      return this;
    }

    public Builder withTotalTasks(int numTasks) {
      request.setTotalTasks(numTasks);
      return this;
    }

    public Builder withBandwidthPerTasks(int bandwidth) {
      request.setBandwidth(bandwidth);
      return this;
    }

    public Builder withNoChecksumVerify(boolean noChecksumVerify) {
      request.setNoChecksumVerify(noChecksumVerify);
      return this;
    }

    public Builder withYarnPoolName(String name) {
      request.setYarnPoolName(name);
      return this;
    }

    public Builder withContinuousBackupEnabled(boolean continuousBackupEnabled) {
      request.setContinuousBackupEnabled(continuousBackupEnabled);
      return this;
    }

    public BackupRequest build() {
      return request;
    }

  }

  private BackupType type;
  private List<TableName> tableList;
  private String targetRootDir;
  private int totalTasks = -1;
  private long bandwidth = -1L;
  private boolean noChecksumVerify = false;
  private String backupSetName;
  private String yarnPoolName;
  private boolean continuousBackupEnabled;

  private BackupRequest() {
  }

  private BackupRequest setBackupType(BackupType type) {
    this.type = type;
    return this;
  }

  public BackupType getBackupType() {
    return this.type;
  }

  private BackupRequest setTableList(List<TableName> tableList) {
    this.tableList = tableList;
    return this;
  }

  public List<TableName> getTableList() {
    return this.tableList;
  }

  private BackupRequest setTargetRootDir(String targetRootDir) {
    this.targetRootDir = targetRootDir;
    return this;
  }

  public String getTargetRootDir() {
    return this.targetRootDir;
  }

  private BackupRequest setTotalTasks(int totalTasks) {
    this.totalTasks = totalTasks;
    return this;
  }

  public int getTotalTasks() {
    return this.totalTasks;
  }

  private BackupRequest setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
    return this;
  }

  public long getBandwidth() {
    return this.bandwidth;
  }

  private BackupRequest setNoChecksumVerify(boolean noChecksumVerify) {
    this.noChecksumVerify = noChecksumVerify;
    return this;
  }

  public boolean getNoChecksumVerify() {
    return noChecksumVerify;
  }

  public String getBackupSetName() {
    return backupSetName;
  }

  private BackupRequest setBackupSetName(String backupSetName) {
    this.backupSetName = backupSetName;
    return this;
  }

  public String getYarnPoolName() {
    return yarnPoolName;
  }

  public void setYarnPoolName(String yarnPoolName) {
    this.yarnPoolName = yarnPoolName;
  }

  private void setContinuousBackupEnabled(boolean continuousBackupEnabled) {
    this.continuousBackupEnabled = continuousBackupEnabled;
  }

  public boolean getContinuousBackupEnabled() {
    return this.continuousBackupEnabled;
  }
}
