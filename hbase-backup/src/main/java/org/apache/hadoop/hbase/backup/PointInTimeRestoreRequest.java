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

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * POJO class for Point In Time Restore request
 */
@InterfaceAudience.Private
public final class PointInTimeRestoreRequest {

  private final String backupRootDir;
  private final String restoreRootDir;
  private final boolean check;
  private final TableName[] fromTables;
  private final TableName[] toTables;
  private final boolean overwrite;
  private final long toDateTime;
  private final boolean isKeepOriginalSplits;

  private PointInTimeRestoreRequest(Builder builder) {
    this.backupRootDir = builder.backupRootDir;
    this.restoreRootDir = builder.restoreRootDir;
    this.check = builder.check;
    this.fromTables = builder.fromTables;
    this.toTables = builder.toTables;
    this.overwrite = builder.overwrite;
    this.toDateTime = builder.toDateTime;
    this.isKeepOriginalSplits = builder.isKeepOriginalSplits;
  }

  public String getBackupRootDir() {
    return backupRootDir;
  }

  public String getRestoreRootDir() {
    return restoreRootDir;
  }

  public boolean isCheck() {
    return check;
  }

  public TableName[] getFromTables() {
    return fromTables;
  }

  public TableName[] getToTables() {
    return toTables;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public long getToDateTime() {
    return toDateTime;
  }

  public boolean isKeepOriginalSplits() {
    return isKeepOriginalSplits;
  }

  public static class Builder {
    private String backupRootDir;
    private String restoreRootDir;
    private boolean check = false;
    private TableName[] fromTables;
    private TableName[] toTables;
    private boolean overwrite = false;
    private long toDateTime;
    private boolean isKeepOriginalSplits;

    public Builder withBackupRootDir(String backupRootDir) {
      this.backupRootDir = backupRootDir;
      return this;
    }

    public Builder withRestoreRootDir(String restoreRootDir) {
      this.restoreRootDir = restoreRootDir;
      return this;
    }

    public Builder withCheck(boolean check) {
      this.check = check;
      return this;
    }

    public Builder withFromTables(TableName[] fromTables) {
      this.fromTables = fromTables;
      return this;
    }

    public Builder withToTables(TableName[] toTables) {
      this.toTables = toTables;
      return this;
    }

    public Builder withOverwrite(boolean overwrite) {
      this.overwrite = overwrite;
      return this;
    }

    public Builder withToDateTime(long dateTime) {
      this.toDateTime = dateTime;
      return this;
    }

    public Builder withKeepOriginalSplits(boolean isKeepOriginalSplits) {
      this.isKeepOriginalSplits = isKeepOriginalSplits;
      return this;
    }

    public PointInTimeRestoreRequest build() {
      return new PointInTimeRestoreRequest(this);
    }
  }
}
